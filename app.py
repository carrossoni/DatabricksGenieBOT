"""
Databricks Genie Bot

Author: Luiz Carrossoni Neto
Revision: 1.0

This script implements an experimental chatbot that interacts with Databricks' Genie API. The bot facilitates conversations with Genie,
Databricks' AI assistant, through a chat interface.

Note: This is experimental code and is not intended for production use.


Update on May 02 to reflect Databricks API Changes https://www.databricks.com/blog/genie-conversation-apis-public-preview
"""

import os
import json
import logging
import time
from typing import Dict, List, Optional
from dotenv import load_dotenv
from aiohttp import web
from botbuilder.core import BotFrameworkAdapterSettings, BotFrameworkAdapter, ActivityHandler, TurnContext
from botbuilder.schema import Activity, ChannelAccount
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieAPI, MessageStatus
import asyncio
import requests

# Log for prod
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)
# logging.getLogger("databricks_genie").setLevel(logging.DEBUG)

#Log for development
# 1) Enable DEBUG everywhere (you can narrow this later)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
)

# 2) Your module logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# 3) Turn on the Databricks SDK + HTTP internals
logging.getLogger("databricks").setLevel(logging.DEBUG)
logging.getLogger("databricks.sdk").setLevel(logging.DEBUG)
logging.getLogger("urllib3").setLevel(logging.DEBUG)

# Env vars
load_dotenv()

DATABRICKS_SPACE_ID = os.getenv("DATABRICKS_SPACE_ID")
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
APP_ID = os.getenv("MicrosoftAppId", "")
APP_PASSWORD = os.getenv("MicrosoftAppPassword", "")

workspace_client = WorkspaceClient(
    host=DATABRICKS_HOST,
    token=DATABRICKS_TOKEN
)


# 2) Register healthz **before** all your other routes
async def healthz(request):
    return web.Response(status=200)

app = web.Application()
app.router.add_get("/healthz", healthz)

genie_api = GenieAPI(workspace_client.api_client)

def get_attachment_query_result(space_id, conversation_id, message_id, attachment_id):
    url = f"{DATABRICKS_HOST}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        logger.error(f"Message endpoint returned status {response.status_code}: {response.text}")
        return {}
    
    try:
        message_data = response.json()
        logger.info(f"Message data: {message_data}")
        
        statement_id = None
        if "attachments" in message_data:
            for attachment in message_data["attachments"]:
                if attachment.get("attachment_id") == attachment_id:
                    if "query" in attachment and "statement_id" in attachment["query"]:
                        statement_id = attachment["query"]["statement_id"]
                        break
        
        if not statement_id:
            logger.error("No statement_id found in message data")
            return {}
            
        query_url = f"{DATABRICKS_HOST}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/attachments/{attachment_id}/query-result"
        query_headers = {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/json",
            "X-Databricks-Statement-Id": statement_id
        }
        
        query_response = requests.get(query_url, headers=query_headers)
        if query_response.status_code != 200:
            logger.error(f"Query result endpoint returned status {query_response.status_code}: {query_response.text}")
            return {}
            
        if not query_response.text.strip():
            logger.error(f"Empty response from Genie API: {query_response.status_code}")
            return {}
            
        result = query_response.json()
        logger.info(f"Raw query result response: {result}")
        
        if isinstance(result, dict):
            if "data_array" in result:
                if not isinstance(result["data_array"], list):
                    result["data_array"] = []
            if "schema" in result:
                if not isinstance(result["schema"], dict):
                    result["schema"] = {}
                    
            if "schema" in result and "columns" in result["schema"]:
                if not isinstance(result["schema"]["columns"], list):
                    result["schema"]["columns"] = []
                    
            if "data_array" in result and result["data_array"] and "schema" not in result:
                first_row = result["data_array"][0]
                if isinstance(first_row, dict):
                    result["schema"] = {
                        "columns": [{"name": key} for key in first_row.keys()]
                    }
                elif isinstance(first_row, list):
                    result["schema"] = {
                        "columns": [{"name": f"Column {i}"} for i in range(len(first_row))]
                    }
                    
        return result
    except Exception as e:
        logger.error(f"Failed to process Genie API response: {e}, text: {response.text}")
        return {}

def execute_attachment_query(space_id, conversation_id, message_id, attachment_id, payload):
    url = f"{DATABRICKS_HOST}/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/attachments/{attachment_id}/execute-query"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code != 200:
        logger.error(f"Execute query endpoint returned status {response.status_code}: {response.text}")
        return {}
    if not response.text.strip():
        logger.error(f"Empty response from Genie API: {response.status_code}")
        return {}
    try:
        return response.json()
    except Exception as e:
        logger.error(f"Failed to parse JSON from Genie API: {e}, text: {response.text}")
        return {}



logger = logging.getLogger(__name__)

async def ask_genie(
    question: str,
    space_id: str,
    conversation_id: Optional[str] = None
) -> tuple[str, str]:
    logger.debug("🔥 ENTERING ask_genie v2! 🔥")
    logger.debug("🛠️  ASK_GENIE PAYLOAD HOTFIX DEPLOYED 🛠️")
    try:
        loop = asyncio.get_running_loop()

        # ───────────────────────────────────────────────────────────────────────────
        # 1) start or continue conversation
        # ───────────────────────────────────────────────────────────────────────────
        if conversation_id is None:
            initial_message = await loop.run_in_executor(
                None, genie_api.start_conversation_and_wait, space_id, question
            )
            conversation_id = initial_message.conversation_id
        else:
            initial_message = await loop.run_in_executor(
                None,
                genie_api.create_message_and_wait,
                space_id, conversation_id, question
            )

        message_id = initial_message.message_id

        # ───────────────────────────────────────────────────────────────────────────
        # 2) poll for COMPLETED with exponential backoff
        # ───────────────────────────────────────────────────────────────────────────
        max_attempts = 5
        backoff_base = 2
        for attempt in range(1, max_attempts + 1):
            message_content = await loop.run_in_executor(
                None,
                genie_api.get_message,
                space_id, conversation_id, message_id
            )
            status = getattr(message_content, "status", None)
            logger.debug(f"[Poll {attempt}/{max_attempts}] status={status}")

            if status == MessageStatus.COMPLETED:
                break
            if status == MessageStatus.FAILED:
                err = getattr(message_content, "error_message", "<no error>")
                logger.error(f"Genie FAILED on attempt {attempt}: {err}")
            else:
                logger.debug(f"Sleeping {backoff_base**attempt}s before retry")

            if attempt < max_attempts:
                time.sleep(backoff_base ** attempt)
            else:
                raise RuntimeError(f"Genie did not complete after {max_attempts} attempts")

        logger.info(f"Raw message content: {message_content}")

        # ───────────────────────────────────────────────────────────────────────────
        # 3) handle any plain‑text attachments first
        # ───────────────────────────────────────────────────────────────────────────
        if message_content.attachments:
            for attachment in message_content.attachments:
                text_obj = getattr(attachment, "text", None)
                if text_obj and hasattr(text_obj, "content"):
                    return json.dumps({"message": text_obj.content}), conversation_id

                # # ───────────────────────────────────────────────────────────────────────
                # # 3b) handle the SQL card attachment
                # # ───────────────────────────────────────────────────────────────────────
                # attachment_id = getattr(attachment, "attachment_id", None)
                # query_obj     = getattr(attachment, "query", None)
                # if attachment_id and query_obj:
                #     # pull description & raw SQL from the SDK object
                #     desc    = getattr(query_obj, "description", "")
                #     raw_sql = getattr(query_obj, "query", "")

                #     # fetch the actual query‑result JSON
                #     query_result = await loop.run_in_executor(
                #         None,
                #         get_attachment_query_result,
                #         space_id,
                #         conversation_id,
                #         message_id,
                #         attachment_id
                #     )

                #     logger.debug(f"Fetched query_result: {query_result!r}")

                #     # collapse the SQL into <details> if we got raw_sql
                #     sql_block = ""
                #     if raw_sql:
                #         sql_block = (
                #             "<details>\n"
                #             "  <summary><b>View generated SQL</b></summary>\n\n"
                #             "```sql\n"
                #             f"{raw_sql.strip()}\n"
                #             "```\n"
                #             "</details>\n"
                #         )

                #     # build the ONE payload your renderer expects
                #     payload = {
                #         "query_description":     desc,
                #         "query_result_metadata": query_result.get("query_result_metadata", {}),
                #         "statement_response": {
                #             # pass through the full dict that contains both data_array & schema
                #             "result": query_result
                #         },
                #         # only include if we actually have SQL to show
                #         **({"raw_sql_markdown": sql_block} if sql_block else {})
                #     }

                #     logger.debug("🚀  FINAL GENIE PAYLOAD: %s", payload)
                #     return json.dumps(payload), conversation_id

                # 3b) A SQL card attachment
                attachment_id = getattr(attachment, "attachment_id", None)
                query_obj     = getattr(attachment, "query", None)

                if attachment_id and query_obj:
                    # 1) grab description & raw SQL
                    desc    = getattr(query_obj, "description", "")
                    raw_sql = getattr(query_obj, "query", "")

                    # 2) fetch the SDK’s full query‐result payload
                    query_result = await loop.run_in_executor(
                        None,
                        get_attachment_query_result,
                        space_id,
                        conversation_id,
                        message_id,
                        attachment_id
                    )

                    # 3) pull out rows & columns in a version‐agnostic way:
                    #    if they shipped nested under "result", drill in; otherwise top‐level
                    inner = query_result.get("result") or {}
                    rows = inner.get("data_array", query_result.get("data_array", []))

                    #    columns may live under manifest.schema or directly under a top‐level schema
                    manifest = query_result.get("manifest", {})
                    cols = (
                        manifest.get("schema", {}).get("columns")
                        or query_result.get("schema", {}).get("columns")
                        or []
                    )

                    # 4) build the collapsible SQL block
                    markdown_sql = (
                        "<details>\n"
                        "  <summary><b>View generated SQL</b></summary>\n\n"
                        "```sql\n"
                        f"{raw_sql.strip()}\n"
                        "```\n"
                        "</details>"
                    ) if raw_sql else None

                    # 5) assemble exactly the two lists your renderer expects:
                    final_payload = {
                        "query_description":     desc,
                        "query_result_metadata": query_result.get("query_result_metadata", {}),
                        "statement_response": {
                            "result": {
                                "data_array": rows,
                                "schema": {"columns": cols}
                            }
                        },
                        **({"raw_sql_markdown": markdown_sql} if markdown_sql else {})
                    }

                    logger.debug("🚀 FINAL GENIE PAYLOAD: %s", final_payload)
                    return json.dumps(final_payload), conversation_id


        # ───────────────────────────────────────────────────────────────────────────
        # 4) no attachments → fallback
        # ───────────────────────────────────────────────────────────────────────────
        return json.dumps({"error": "No data available."}), conversation_id

    except Exception as e:
        logger.error(f"Error in ask_genie: {e}")
        return json.dumps({"error": "An error occurred while processing your request."}), conversation_id



def process_query_results(answer_json: Dict) -> str:
    sections: List[str] = []
    logger.info(f"Processing answer JSON: {answer_json}")

    # ─────────────────────────────────────────────
    # 1) Query Description (if provided)
    # ─────────────────────────────────────────────
    if "query_description" in answer_json and answer_json["query_description"]:
        sections.append(f"## Query Description\n\n{answer_json['query_description']}\n")

    # ─────────────────────────────────────────────
    # 2) Metadata (row count, execution time)
    # ─────────────────────────────────────────────
    metadata = answer_json.get("query_result_metadata", {})
    if isinstance(metadata, dict):
        meta_lines = []
        if "row_count" in metadata:
            meta_lines.append(f"**Row Count:** {metadata['row_count']}")
        if "execution_time_ms" in metadata:
            meta_lines.append(f"**Execution Time:** {metadata['execution_time_ms']}ms")
        if meta_lines:
            sections.append("\n".join(meta_lines) + "\n")

    # ─────────────────────────────────────────────
    # 3) Query Results Table
    # ─────────────────────────────────────────────
    stmt   = answer_json.get("statement_response", {})
    result = stmt.get("result", {})
    rows   = result.get("data_array", [])
    schema = result.get("schema", {}).get("columns", [])

    if rows and schema:
        # header row
        header = "| " + " | ".join(col["name"] for col in schema) + " |"
        sep    = "|" + "|".join(" --- " for _ in schema) + "|"
        table_lines = [header, sep]

        # data rows
        for row in rows:
            formatted = []
            for val, col in zip(row, schema):
                if val is None:
                    formatted.append("NULL")
                elif col.get("type_name") in ("DECIMAL", "DOUBLE", "FLOAT"):
                    formatted.append(f"{float(val):,.2f}")
                elif col.get("type_name") in ("INT", "BIGINT", "LONG"):
                    formatted.append(f"{int(val):,}")
                else:
                    formatted.append(str(val))
            table_lines.append("| " + " | ".join(formatted) + " |")

        sections.append("## Query Results\n\n" + "\n".join(table_lines) + "\n")
    else:
        logger.debug("No results table to render")

    # ─────────────────────────────────────────────
    # 4) Collapsible raw SQL (if we captured it)
    # ─────────────────────────────────────────────
    raw_sql_md = answer_json.get("raw_sql_markdown")
    if raw_sql_md:
        sections.append(raw_sql_md)

    # ─────────────────────────────────────────────
    # Fallback: if absolutely nothing added
    # ─────────────────────────────────────────────
    if not sections:
        logger.error("No data available to show in process_query_results")
        return "No data available.\n\n"

    # join all the pieces with blank lines between them
    return "\n".join(sections)

SETTINGS = BotFrameworkAdapterSettings(APP_ID, APP_PASSWORD
                                       )
ADAPTER = BotFrameworkAdapter(SETTINGS)

class MyBot(ActivityHandler):
    def __init__(self):
        self.conversation_ids: Dict[str, str] = {}

    async def on_message_activity(self, turn_context: TurnContext):
        user_id = turn_context.activity.from_property.id
        question = turn_context.activity.text

        try:
            # 1) call Genie
            answer, new_conversation_id = await ask_genie(
                question,
                DATABRICKS_SPACE_ID,
                self.conversation_ids.get(user_id),
            )
            self.conversation_ids[user_id] = new_conversation_id

            # 2) parse & render
            answer_json = json.loads(answer)
            response    = process_query_results(answer_json)

            # 3) send it once
            logger.info(f"Sending response: {response!r}")
            await turn_context.send_activity(response)

        except json.JSONDecodeError as jde:
            logger.exception("Failed to parse JSON from Genie")
            await turn_context.send_activity(
                "⚠️ I got something I couldn’t understand back from Genie."
            )

        except Exception as e:
            # **this will now log the full stacktrace to your container logs**
            logger.exception("Unhandled error in on_message_activity")
            await turn_context.send_activity(
                "❗️ I’m sorry—I ran into an unexpected error processing your request. "
                "Please try again in a moment."
            )

    async def on_members_added_activity(self, members_added: List[ChannelAccount], turn_context: TurnContext):
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                await turn_context.send_activity("Welcome to the Supply Chain KNOWLEDGE Agent!")

BOT = MyBot()

async def messages(req: web.Request) -> web.Response:
    if "application/json" in req.headers["Content-Type"]:
        body = await req.json()
    else:
        return web.Response(status=415)

    activity = Activity().deserialize(body)
    auth_header = req.headers.get("Authorization", "")

    try:
        response = await ADAPTER.process_activity(activity, auth_header, BOT.on_turn)
        if response:
            return web.json_response(data=response.body, status=response.status)
        return web.Response(status=201)
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return web.Response(status=500)

app.router.add_post("/api/messages", messages)

if __name__ == "__main__":
    try:
        host = os.getenv("HOST", "localhost")
        port = int(os.environ.get("PORT", 3978))
        web.run_app(app, host=host, port=port)
    except Exception as error:
        logger.exception("Error running app")
