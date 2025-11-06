"""
Databricks Genie Bot

Authors: Luiz Carrossoni Neto, Ryan Bates
Revision: 1.2

This script implements an experimental chatbot that interacts with Databricks' Genie API. The bot facilitates conversations with Genie,
Databricks' AI assistant, through a chat interface.

Note: This is experimental code and is not intended for production use.
This is a test


Update on May 2 to reflect Databricks API Changes https://www.databricks.com/blog/genie-conversation-apis-public-preview
Update on Aug 5 to reflect Microsoft Azure no longer supporting MultiTenant bots
Update on Oct 8 to add in user session management, user context, and feedback to the Genie API
"""

"""
Startup Command:
python3 -m aiohttp.web -H 0.0.0.0 -P 8000 app:init_func

"""

from asyncio.log import logger
import os
import json
import logging
from typing import Dict, List, Optional
from dotenv import load_dotenv
from aiohttp import web
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieAPI
import asyncio
import sys
import traceback
from datetime import datetime, timezone, timedelta
from http import HTTPStatus
from aiohttp.web import Request, Response, json_response
from botbuilder.core import (
    BotFrameworkAdapterSettings,
    BotFrameworkAdapter,
    ActivityHandler,
    TurnContext,
)
from botbuilder.core.integration import aiohttp_error_middleware
from botbuilder.integration.aiohttp import (
    CloudAdapter,
    ConfigurationBotFrameworkAuthentication,
)
from botbuilder.schema import (
    Activity,
    ConversationReference,
    ActivityTypes,
    ChannelAccount,
    InvokeResponse,
)
import requests
import re

from config import DefaultConfig
from botbuilder.core.teams import TeamsInfo

CONFIG = DefaultConfig()


class UserSession:
    """Represents a user session with email-based identification"""
    def __init__(self, user_id: str, email: str, name: str = None):
        self.user_id = user_id  # Teams user ID
        self.email = email
        self.name = name or email.split('@')[0]  # Use email prefix as name if no name provided
        self.conversation_id = None
        self.created_at = datetime.now(timezone.utc)
        self.last_activity = datetime.now(timezone.utc)
        self.is_authenticated = True  # Always true for Teams users
        self.user_context = {}
    
    def update_activity(self):
        """Update the last activity timestamp"""
        self.last_activity = datetime.now(timezone.utc)
    
    def to_dict(self):
        """Convert session to dictionary for logging/debugging"""
        return {
            "user_id": self.user_id,
            "email": self.email,
            "name": self.name,
            "conversation_id": self.conversation_id,
            "created_at": self.created_at.isoformat(),
            "last_activity": self.last_activity.isoformat(),
            "is_authenticated": self.is_authenticated
        }
    
    def get_display_name(self):
        """Get a friendly display name for the user"""
        return f"{self.name} ({self.email})"

# For local development with Bot Framework Emulator, use BotFrameworkAdapter
if CONFIG.APP_ID and CONFIG.APP_PASSWORD:
    # Production: Use CloudAdapter
    ADAPTER = CloudAdapter(ConfigurationBotFrameworkAuthentication(CONFIG))
else:
    # Local testing: Use BotFrameworkAdapter with empty credentials
    SETTINGS = BotFrameworkAdapterSettings("", "")
    ADAPTER = BotFrameworkAdapter(SETTINGS)


async def on_error(context: TurnContext, error: Exception):
    import traceback
    stack = traceback.format_exc()
    logger.error(f"❌ Unhandled error in bot: {error}\n{stack}")
    # Optional: echo a safe message back to Teams (for testing only)
    try:
        await context.send_activity(f"⚠️ Internal bot error:\n```\n{error}\n```")
    except Exception:
        pass


ADAPTER.on_turn_error = on_error

# Initialize Databricks client with error handling
def get_databricks_client():
    """Get Databricks WorkspaceClient with proper error handling"""

    try:
        # Debug environment variable loading
        logger.info(f"Loading Databricks configuration...")
        logger.info(f"DATABRICKS_HOST: {CONFIG.DATABRICKS_HOST}")
        logger.info(f"DATABRICKS_TOKEN present: {bool(CONFIG.DATABRICKS_TOKEN)}")
        logger.info(f"DATABRICKS_TOKEN length: {len(CONFIG.DATABRICKS_TOKEN) if CONFIG.DATABRICKS_TOKEN else 0}")
        
        if not CONFIG.DATABRICKS_TOKEN:
            raise ValueError("DATABRICKS_TOKEN environment variable is not set")
        
        client = WorkspaceClient(
            host=CONFIG.DATABRICKS_HOST, 
            token=CONFIG.DATABRICKS_TOKEN
        )
        logger.info("Databricks client initialized successfully")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize Databricks client: {str(e)}")
        raise

# Initialize clients
workspace_client = get_databricks_client()
genie_api = GenieAPI(workspace_client.api_client)


async def ask_genie(
    question: str, space_id: str, user_session: UserSession, conversation_id: Optional[str] = None
) -> tuple[str, str, str]:
    try:
        # Add user context to the question for better tracking in Databricks
        contextual_question = f"[{user_session.email}] {question}"
        
        loop = asyncio.get_running_loop()
        if conversation_id is None:
            # Start a new conversation
            initial_message = await loop.run_in_executor(
                None, genie_api.start_conversation_and_wait, space_id, contextual_question
            )
            conversation_id = initial_message.conversation_id
        else:
            # Continue existing conversation with a new message
            initial_message = await loop.run_in_executor(
                None, genie_api.create_message_and_wait, space_id, conversation_id, contextual_question
            )
           
        query_result = None
        if initial_message.query_result is not None:
            query_result = await loop.run_in_executor(
                None,
                genie_api.get_message_attachment_query_result,
                #genie_api.get_message_query_result,
                space_id,
                initial_message.conversation_id,
                initial_message.message_id,
                initial_message.attachments[0].attachment_id,
           )
        message_content = await loop.run_in_executor(
            None,
            genie_api.get_message,
            space_id,
            initial_message.conversation_id,
            initial_message.message_id,
        )
        if query_result and query_result.statement_response:
            results = await loop.run_in_executor(
                None,
                workspace_client.statement_execution.get_statement,
                query_result.statement_response.statement_id,
            )

            query_description = ""
            for attachment in message_content.attachments:
                if attachment.query and attachment.query.description:
                    query_description = attachment.query.description
                    break

            return (
                json.dumps(
                    {
                        "columns": results.manifest.schema.as_dict(),
                        "data": results.result.as_dict(),
                        "query_description": query_description,
                    }
                ),
                conversation_id,
                initial_message.message_id,
            )

        if message_content.attachments:
            for attachment in message_content.attachments:
                if attachment.text and attachment.text.content:
                    return (
                        json.dumps({"message": attachment.text.content}),
                        conversation_id,
                        initial_message.message_id,
                    )

        return json.dumps({"message": message_content.content}), conversation_id, initial_message.message_id
    except Exception as e:
        error_str = str(e).lower()  # Convert to lowercase for case-insensitive matching
        error_original = str(e)  # Keep original for logging
        logger.error(f"Error in ask_genie for user {user_session.get_display_name()}: {error_original}")
        
        # Check for IP ACL blocking - look for "blocked" + "ip acl" pattern
        # Error message format: "Source IP address: X.X.X.X is blocked by Databricks IP ACL for workspace"
        if "ip acl" in error_str and "blocked" in error_str:
            logger.error(f"IP ACL blocking detected: {error_original}")
            return (
                json.dumps({
                    "error": "⚠️ **IP Access Blocked**\n\n"
                            "The bot's IP address is blocked by Databricks Account IP Access Control Lists (ACLs).\n\n"
                            "**Administrator Action Required:**\n"
                            "Please check the TROUBLESHOOTING.md documentation for instructions on adding "
                            "the bot's IP address to your Databricks Account IP allow list."
                }),
                conversation_id,
                None,
            )
        
        # Generic error for other cases
        return (
            json.dumps({"error": "An error occurred while processing your request."}),
            conversation_id,
            None,
        )


def process_query_results(answer_json: Dict) -> str:
    response = ""
    if "query_description" in answer_json and answer_json["query_description"]:
        response += f"## Query Description\n\n{answer_json['query_description']}\n\n"

    if "columns" in answer_json and "data" in answer_json:
        response += "## Query Results\n\n"
        columns = answer_json["columns"]
        data = answer_json["data"]
        if isinstance(columns, dict) and "columns" in columns:
            header = "| " + " | ".join(col["name"] for col in columns["columns"]) + " |"
            separator = "|" + "|".join(["---" for _ in columns["columns"]]) + "|"
            response += header + "\n" + separator + "\n"
            for row in data["data_array"]:
                formatted_row = []
                for value, col in zip(row, columns["columns"]):
                    if value is None:
                        formatted_value = "NULL"
                    elif col["type_name"] in ["DECIMAL", "DOUBLE", "FLOAT"]:
                        formatted_value = f"{float(value):,.2f}"
                    elif col["type_name"] in ["INT", "BIGINT", "LONG"]:
                        formatted_value = f"{int(value):,}"
                    else:
                        formatted_value = str(value)
                    formatted_row.append(formatted_value)
                response += "| " + " | ".join(formatted_row) + " |\n"
        else:
            response += f"Unexpected column format: {columns}\n\n"
    elif "error" in answer_json:
        response += f"{answer_json['error']}\n\n"
    elif "message" in answer_json:
        response += f"{answer_json['message']}\n\n"
    else:
        response += "No data available.\n\n"

    return response


class MyBot(ActivityHandler):
    def __init__(self):
        self.user_sessions: Dict[str, UserSession] = {}  # Maps Teams user ID to UserSession
        self.email_sessions: Dict[str, UserSession] = {}  # Maps email to UserSession for easy lookup
        self.message_feedback: Dict[str, Dict] = {}  # Track feedback for each message
        self.pending_email_input: Dict[str, bool] = {}  # Track users waiting for email input

    async def get_or_create_user_session(self, turn_context: TurnContext) -> UserSession:
        """Get or create a user session based on Teams user information"""

        user_id = turn_context.activity.from_property.id
        
        # Check if we already have a session for this user
        if user_id in self.user_sessions:
            session = self.user_sessions[user_id]
            
            # Check if conversation has timed out (4 hours)
            if self._is_conversation_timed_out(session):
                logger.info(f"Conversation timed out for user {session.get_display_name()}, resetting conversation")
                # Reset conversation ID and user context to start fresh
                session.conversation_id = None
                session.user_context.pop('last_conversation_id', None)
                # Update activity time
                session.update_activity()
                return session
            else:
                # Update activity time for active session
                session.update_activity()
                return session

        email, name = None, None

        # --- 1️⃣ Try get_member() ---
        try:
            member = await TeamsInfo.get_member(turn_context, user_id)
            email = getattr(member, "email", None) or getattr(member, "userPrincipalName", None)
            name = getattr(member, "name", None)
            logger.info(f"✅ Found Teams member via get_member(): {name} ({email})")
        except Exception as e:
            logger.warning(f"get_member() failed for {user_id}: {e}")

        # --- 2️⃣ If still no email, try get_members() (helps 1:1 chats) ---
        if not email:
            try:
                members = await TeamsInfo.get_members(turn_context)
                if members and isinstance(members, list):
                    for m in members:
                        # Skip the bot itself
                        if m.id != turn_context.activity.recipient.id:
                            email = getattr(m, "email", None) or getattr(m, "userPrincipalName", None)
                            name = getattr(m, "name", None)
                            logger.info(f"✅ Found Teams member via get_members(): {name} ({email})")
                            break
            except Exception as e:
                logger.warning(f"get_members() also failed for {user_id}: {e}")

        # --- 3️⃣ Final fallback if Teams provides nothing ---
        if not email:
            email = "NoEmail"
            name = getattr(turn_context.activity.from_property, "name", None) or "Unknown User"
            logger.info(f"⚠️ No Teams email found for {user_id}. Using placeholder 'NoEmail'.")

        # --- 4️⃣ Create and store session ---
        session = UserSession(user_id, email, name or email.split("@")[0])
        self.user_sessions[user_id] = session
        self.email_sessions[email] = session
        logger.info(f"✅ Created new user session for {session.get_display_name()}")
        return session

    def _is_valid_email(self, email: str) -> bool:
        """Validate email address format"""
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(email_pattern, email) is not None

    def _is_conversation_timed_out(self, user_session: UserSession) -> bool:
        """Check if conversation has timed out (4 hours)"""
        if not user_session:
            return False
        
        time_since_last_activity = datetime.now(timezone.utc) - user_session.last_activity
        timeout_threshold = timedelta(hours=4)
        
        return time_since_last_activity > timeout_threshold

    def _get_sample_questions(self) -> List[str]:
        """Get sample questions from configuration"""
        # Parse sample questions from config (semicolon-delimited)
        questions_str = CONFIG.SAMPLE_QUESTIONS
        if questions_str:
            # Split by semicolon and strip whitespace
            questions = [q.strip() for q in questions_str.split(';') if q.strip()]
            return questions if questions else [
                "What data is available?",
                "Can you explain the datasets?",
                "What questions should I ask?"
            ]
        else:
            # Fallback default questions
            return [
                "What data is available?",
                "Can you explain the datasets?",
                "What questions should I ask?"
            ]

    # async def _create_session_with_manual_email(self, turn_context: TurnContext, email: str) -> UserSession:
    #     """Create a user session with manually provided email"""
    #     user_id = turn_context.activity.from_property.id
    #     user_name = getattr(turn_context.activity.from_property, 'name', None) or email.split('@')[0]
        
    #     # Create new user session
    #     session = UserSession(user_id, email, user_name)
    #     self.user_sessions[user_id] = session
    #     self.email_sessions[email] = session
        
    #     # Remove from pending email input
    #     if user_id in self.pending_email_input:
    #         del self.pending_email_input[user_id]
        
    #     logger.info(f"Created user session with manual email for {session.get_display_name()}")
    #     return session

    def create_feedback_card(self, message_id: str, user_id: str) -> Dict:
        """Create an Adaptive Card with thumbs up/down feedback buttons"""
        return {
            "type": "AdaptiveCard",
            "version": "1.3",
            "body": [
                {
                    "type": "TextBlock",
                    "text": "Was this response helpful?",
                    "size": "Small",
                    "color": "Default"
                }
            ],
            "actions": [
                {
                    "type": "Action.Submit",
                    "title": "👍",
                    "data": {
                        "action": "feedback",
                        "messageId": message_id,
                        "userId": user_id,
                        "feedback": "positive"
                    }
                },
                {
                    "type": "Action.Submit",
                    "title": "👎",
                    "data": {
                        "action": "feedback",
                        "messageId": message_id,
                        "userId": user_id,
                        "feedback": "negative"
                    }
                }
            ]
        }

    def create_thank_you_card(self) -> Dict:
        """Create a thank you card to replace feedback buttons after submission"""
        return {
            "type": "AdaptiveCard",
            "version": "1.3",
            "body": [
                {
                    "type": "TextBlock",
                    "text": "✅ Thank you for your feedback!",
                    "size": "Small",
                    "color": "Good"
                }
            ]
        }

    def create_error_card(self, error_message: str) -> Dict:
        """Create an error card to show when feedback submission fails"""
        return {
            "type": "AdaptiveCard",
            "version": "1.3",
            "body": [
                {
                    "type": "TextBlock",
                    "text": f"❌ {error_message}",
                    "size": "Small",
                    "color": "Attention"
                }
            ]
        }

    async def on_message_activity(self, turn_context: TurnContext):
        # Debug logging for all message activities
        logger.info(f"Message activity type: {turn_context.activity.type}")
        logger.info(f"Message activity name: {turn_context.activity.name}")
        logger.info(f"Message activity value: {turn_context.activity.value}")
        logger.info(f"Message activity text: {turn_context.activity.text}")
        
        # Handle cases where text might be None (e.g., adaptive card interactions)
        if not turn_context.activity.text:
            # Check if this is an adaptive card button click
            if turn_context.activity.value and isinstance(turn_context.activity.value, dict):
                action = turn_context.activity.value.get("action")
                if action == "feedback":
                    logger.info("Detected adaptive card feedback button click in message activity")
                    # Handle as feedback submission
                    try:
                        message_id = turn_context.activity.value.get("messageId")
                        user_id = turn_context.activity.value.get("userId")
                        feedback = turn_context.activity.value.get("feedback")
                        
                        if not all([message_id, user_id, feedback]):
                            logger.error("Missing required feedback data in message activity")
                            return
                        
                        # Store feedback data
                        feedback_key = f"{user_id}_{message_id}"
                        user_session = self.user_sessions.get(user_id)
                        self.message_feedback[feedback_key] = {
                            "message_id": message_id,
                            "user_id": user_id,
                            "feedback": feedback,
                            "conversation_id": user_session.conversation_id if user_session else None,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "user_session": user_session.to_dict() if user_session else None
                        }
                        
                        # Send feedback to Databricks Genie API
                        try:
                            await self._send_feedback_to_api(feedback_key, self.message_feedback[feedback_key])
                            
                            # Send thank you message
                            await turn_context.send_activity("✅ Thank you for your feedback!")
                            
                        except Exception as e:
                            logger.error(f"Failed to send feedback to Genie API: {str(e)}")
                            await turn_context.send_activity("❌ Failed to submit feedback. Please try again.")
                        
                        return
                        
                    except Exception as e:
                        logger.error(f"Error handling feedback in message activity: {str(e)}")
                        return
            
            logger.info("Received message activity without text content, skipping")
            return
            
        question = turn_context.activity.text.strip()
        user_id = turn_context.activity.from_property.id
        
        # # Check if user is waiting to provide email manually
        # if user_id in self.pending_email_input:
        #     if question.lower() == "cancel":
        #         # User wants to cancel email input
        #         del self.pending_email_input[user_id]
        #         await turn_context.send_activity(
        #             "❌ **Email Input Cancelled**\n\n"
        #             "You can try again later by typing any message. I'll ask for your email again if needed."
        #         )
        #         return
        #     elif self._is_valid_email(question):
        #         # User provided a valid email
        #         user_session = await self._create_session_with_manual_email(turn_context, question)
                
        #         # Get sample questions based on space ID
        #         sample_questions = self._get_sample_questions()
        #         questions_text = "\n".join([f"- \"{q}\"" for q in sample_questions])
                
        #         await turn_context.send_activity(
        #             f"✅ **Email Confirmed!**\n\n"
        #             f"Welcome, {user_session.name}! I've successfully logged you in as {user_session.email}.\n\n"
        #             f"{turn_context.activity.from_property}"
        #             f"Now you can ask me questions about your data. Try asking something like:\n"
        #             f"{questions_text}"
        #         )
        #         return
        #     else:
        #         await turn_context.send_activity(
        #             "❌ **Invalid Email Format**\n\n"
        #             "Please provide a valid email address (e.g., john.doe@company.com).\n\n"
        #             "Type `cancel` to stop the email input process."
        #         )
        #         return
        
        # Get or create user session
        user_session = await self.get_or_create_user_session(turn_context)
        
        # # If we couldn't create a session (no email), ask user to identify themselves
            # await self._handle_user_identification(turn_context, question)
            # return
        
        # Handle special commands first (before checking for timeout reset)
        if await self._handle_special_commands(turn_context, question, user_session):
            return
        
        # Check if conversation was reset due to timeout (only for data questions, not commands)
        if user_session.conversation_id is None and user_session.user_id in self.user_sessions:
            # This means the conversation was reset due to timeout
            # await turn_context.send_activity(
            #     "⏰ **Conversation Reset**\n\n"
            #     "Your previous conversation has expired (4+ hours of inactivity). "
            #     "Starting fresh with a new conversation context.\n\n"
            #     "I'm working on your answer now!"
            # )

            await turn_context.send_activity(
                "🤖 **Starting New Conversation...**\n\n"
            )
        
        # Process the message with user context
        try:
            answer, new_conversation_id, genie_message_id = await ask_genie(
                question, CONFIG.DATABRICKS_SPACE_ID, user_session, user_session.conversation_id
            )
            
            # Update user session with new conversation ID and store the specific message ID for feedback
            user_session.conversation_id = new_conversation_id
            user_session.user_context['last_question'] = question
            user_session.user_context['last_response_time'] = datetime.now(timezone.utc).isoformat()
            user_session.user_context['last_genie_message_id'] = genie_message_id

            answer_json = json.loads(answer)
            response = process_query_results(answer_json)
            
            # Add user context to response
            response = f"**👤 {user_session.name}**\n\n{response}"

            # Send the main response
            await turn_context.send_activity(response)
            
            # Send feedback card as a separate message
            await self._send_feedback_card(turn_context, user_session)
            
        except json.JSONDecodeError:
            await turn_context.send_activity(
                f"**👤 {user_session.name}**\n\n❌ Failed to decode response from the server."
            )
            # Send feedback card for error responses too
            await self._send_feedback_card(turn_context, user_session)
        except Exception as e:
            logger.error(f"Error processing message for {user_session.get_display_name()}: {str(e)}")
            await turn_context.send_activity(
                f"**👤 {user_session.name}**\n\n❌ An error occurred while processing your request.\n\n Note: This often occurs when the output is too long, please try adjusting your question to request a shorter answer."
            )

#     async def _handle_user_identification(self, turn_context: TurnContext, question: str):
#         """Handle cases where user email is not available"""
#         user_id = turn_context.activity.from_property.id
        
#         if question.lower() in ["help", "/help", "commands", "/commands"]:
#             help_message = f"""🤖 **Databricks Genie Bot Information**

# **What I do:**
# I'm a Teams bot that connects to a Databricks Genie Space, allowing you to interact with your data through natural language queries directly in Teams.

# **How I work:**
# • I connect to your Databricks workspace using configured credentials
# • Your conversation context is maintained between sessions for continuity
# • I remember our conversation history to provide better follow-up responses

# **Session Management:**
# • Conversations automatically reset after **4 hours** of inactivity
# • You can manually reset anytime by typing `reset` or `new chat`
# • Your email is used **only for logging queries in Genie** - not for AI processing

# **Available Commands:**
# • `help` - Show this information
# • `info` - Get help getting started
# • `whoami` - Display your user information
# • `reset` - Start a fresh conversation
# • `new chat` - Start a fresh conversation
# • `logout` - Clear your session

# **Need Help?**
# Contact the bot administrator at: {CONFIG.ADMIN_CONTACT_EMAIL}"""
            
#             await turn_context.send_activity(help_message)
#         elif question.lower() in ["info", "/info"]:
#             info_text = """🤖 **Welcome to the Genie Bot - User Logging Required**

# I need your email address to log queries in Genie for tracking purposes.

# **Quick Start:**
# - Type `email` to provide your email address
# - I'll validate the format and create your session

# **What happens next:**
# - Once logged in, you can ask me questions about your data
# - I'll remember our conversation context
# - You can ask follow-up questions

# **Learn More:**
# - Type `help` to learn more about the Genie Bot
# - Type `info` for help getting started

# Ready to get started? Type `email` to provide your email address!"""
#             await turn_context.send_activity(info_text)
#         elif question.lower() in ["email", "provide email", "enter email"]:
#             # User wants to provide email manually
#             self.pending_email_input[user_id] = True
#             await turn_context.send_activity(
#                 "📧 **User Logging in Genie**\n\n"
#                 "Please provide your email address (e.g., captain.planet@company.com).\n\n"
#                 "Type `cancel` if you want to stop this process."
#             )
#         else:
#             await turn_context.send_activity(
#                 "🤖 **Welcome to the Genie Bot**\n\n"
#                 "I need your email address to log queries in Genie for tracking purposes.\n\n"
#                 "**Quick Options:**\n"
#                 "- Type `email` to provide your email address\n"
#                 "- Type `help` to learn more about the Genie Bot\n"
#                 "- Type `info` for help getting started\n\n"
#                 "Once logged in, you'll be able to ask me questions about your data!"
#             )

    async def _handle_special_commands(self, turn_context: TurnContext, question: str, user_session: UserSession) -> bool:
        """Handle special commands. Returns True if command was handled."""
        
        # Special emulator command for setting identity
        # if question.lower().startswith("/setuser ") and turn_context.activity.channel_id == "emulator":
        #     # Format: /setuser john.doe@company.com John Doe
        #     parts = question.split(" ", 2)
        #     if len(parts) >= 2:
        #         email = parts[1]
        #         name = parts[2] if len(parts) > 2 else email.split('@')[0]
                
        #         # Update existing session or create new one
        #         user_id = turn_context.activity.from_property.id
        #         session = UserSession(user_id, email, name)
        #         self.user_sessions[user_id] = session
        #         self.email_sessions[email] = session
                
        #         await turn_context.send_activity(
        #             f"✅ **Identity Updated!**\n\n"
        #             f"**Name:** {session.name}\n"
        #             f"**Email:** {session.email}\n\n"
        #             f"You can now ask me questions about your data!"
        #         )
        #         return True
        #     else:
        #         await turn_context.send_activity(
        #             "❌ **Invalid format**\n\n"
        #             "Use: `/setuser your.email@company.com Your Name`\n"
        #             "Example: `/setuser john.doe@company.com John Doe`"
        #         )
        #         return True
        
        # Info command
        if question.lower() in ["info", "/info"]:
            is_emulator = turn_context.activity.channel_id == "emulator"
            
            info_text = f"""🤖 **Databricks Genie Bot Commands**

**👤 User:** {user_session.get_display_name()}

**Start New Conversation:**
- `reset` or `new chat`

**User Commands:**
- `whoami` - Show your user information
- `help` - Show detailed bot information"""

#             if is_emulator:
#                 info_text += """

# **🔧 Emulator Testing Commands:**
# - `/setuser your.email@company.com Your Name` - Set your identity for testing
# - Example: `/setuser john.doe@company.com John Doe`"""

            info_text += f"""

**General Usage:**
- Ask me any question about your data
- I'll remember our conversation context
- Use the commands above to start fresh when needed

**Current Status:** {"New conversation" if user_session.conversation_id is None else "Continuing existing conversation"}

**Need Help?**
Contact the bot administrator at: {CONFIG.ADMIN_CONTACT_EMAIL}"""
            
            await turn_context.send_activity(info_text)
            return True

        # Whoami command
        if question.lower() in ["whoami", "/whoami", "who am i", "me"]:
            user_info = f"""👤 **Your Information**

**Name:** {user_session.name}
**Email:** {user_session.email}
**User ID:** {user_session.user_id}
**Session Created:** {user_session.created_at.strftime('%Y-%m-%d %H:%M:%S UTC')}
**Last Activity:** {user_session.last_activity.strftime('%Y-%m-%d %H:%M:%S UTC')}
**Conversation ID:** {user_session.conversation_id or 'None (new conversation)'}"""
            await turn_context.send_activity(user_info)
            return True

        # # Logout command
        # if question.lower() in ["logout", "/logout", "sign out", "disconnect"]:
        #     # Clear user session
        #     user_id = user_session.user_id
        #     email = user_session.email
            
        #     if user_id in self.user_sessions:
        #         del self.user_sessions[user_id]
        #     if email in self.email_sessions:
        #         del self.email_sessions[email]
            
        #     await turn_context.send_activity(
        #         f"👋 **Goodbye {user_session.name}!**\n\n"
        #         "Your session has been cleared. You'll be re-identified when you send your next message."
        #     )
        #     return True

        # Hello command
        if question.lower() in ["hi", "hello", "start", "hey"]:
            await turn_context.send_activity(
                f"👋 **Hello {user_session.name}!**\n\n"
                "You can ask me questions about your data. Type `help` to see what I can do."
            )

            return True

        # Help command
        if question.lower() in ["help", "/help", "commands", "/commands", "information", "about", "what is this"]:
            help_message = f"""🤖 **Databricks Genie Bot Information**

**What I do:**
I'm a Teams bot that connects to a Databricks Genie Space, allowing you to interact with your data through natural language queries directly in Teams.

**How I work:**
• I connect to your Databricks workspace using configured credentials
• Your conversation context is maintained between sessions for continuity
• I remember our conversation history to provide better follow-up responses

**Session Management:**
• Conversations automatically reset after **4 hours** of inactivity
• You can manually reset anytime by typing `reset` or `new chat`
• Your email is used **only for logging queries in Genie** - not for AI processing

**Available Commands:**
• `help` - Show this information
• `info` - Get help getting started
• `whoami` - Display your user information
• `reset` - Start a fresh conversation
• `new chat` - Start a fresh conversation

**Need Help?**
Contact the bot administrator at: {CONFIG.ADMIN_CONTACT_EMAIL}"""
            
            await turn_context.send_activity(help_message)
            return True

        # New conversation triggers
        new_conversation_triggers = [
            "new conversation", "new chat", "start over", "reset", "clear conversation",
            "/new", "/reset", "/clear", "/start", "begin again", "fresh start", "logout", "/logout"
        ]
        
        if question.lower() in [trigger.lower() for trigger in new_conversation_triggers]:
            user_session.conversation_id = None
            user_session.user_context.pop('last_conversation_id', None)
            await turn_context.send_activity(
                f"🔄 **Starting a new conversation, {user_session.name}!**\n\n"
                "You can now ask me anything about your data."
            )
            return True

        return False

    async def on_invoke_activity(self, turn_context: TurnContext) -> InvokeResponse:
        """Handle invoke activities (like adaptive card button clicks)"""
        try:
            logger.info(f"Received invoke activity: {turn_context.activity.name}")
            logger.info(f"Invoke activity value: {turn_context.activity.value}")
            
            # Check if this is an adaptive card invoke
            if turn_context.activity.name == "adaptiveCard/action":
                invoke_value = turn_context.activity.value
                logger.info(f"Processing adaptive card invoke with value: {invoke_value}")
                return await self.on_adaptive_card_invoke(turn_context, invoke_value)
            
            # Handle other invoke activities if needed
            logger.info(f"Unhandled invoke activity type: {turn_context.activity.name}")
            return InvokeResponse(status_code=200, body="OK")
            
        except Exception as e:
            logger.error(f"Error handling invoke activity: {str(e)}")
            return InvokeResponse(status_code=500, body="Error processing invoke activity")

    async def on_adaptive_card_invoke(self, turn_context: TurnContext, invoke_value: Dict) -> InvokeResponse:
        """Handle Adaptive Card button clicks (feedback submission)"""
        try:
            action = invoke_value.get("action")
            
            if action == "feedback":
                message_id = invoke_value.get("messageId")
                user_id = invoke_value.get("userId")
                feedback = invoke_value.get("feedback")
                
                if not all([message_id, user_id, feedback]):
                    return InvokeResponse(status_code=400, body="Missing required feedback data")
                
                # Store feedback data
                feedback_key = f"{user_id}_{message_id}"
                user_session = self.user_sessions.get(user_id)
                self.message_feedback[feedback_key] = {
                    "message_id": message_id,
                    "user_id": user_id,
                    "feedback": feedback,
                    "conversation_id": user_session.conversation_id if user_session else None,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "user_session": user_session.to_dict() if user_session else None
                }
                
                # Send feedback to Databricks Genie API
                try:
                    await self._send_feedback_to_api(feedback_key, self.message_feedback[feedback_key])
                    
                    # Return updated card with thank you message
                    updated_card = self.create_thank_you_card()
                    
                    return InvokeResponse(
                        status_code=200,
                        body={
                            "type": "AdaptiveCard",
                            "version": "1.3",
                            "body": updated_card["body"]
                        }
                    )
                except Exception as e:
                    logger.error(f"Failed to send feedback to Genie API: {str(e)}")
                    
                    # Return error card
                    error_card = self.create_error_card("Failed to submit feedback. Please try again.")
                    
                    return InvokeResponse(
                        status_code=200,
                        body={
                            "type": "AdaptiveCard",
                            "version": "1.3",
                            "body": error_card["body"]
                        }
                    )
            
            return InvokeResponse(status_code=400, body="Unknown action")
            
        except Exception as e:
            logger.error(f"Error handling adaptive card invoke: {str(e)}")
            return InvokeResponse(status_code=500, body="Error processing feedback")

    async def _send_feedback_to_api(self, feedback_key: str, feedback_data: Dict):
        """Send feedback to Databricks Genie send message feedback API"""
        try:
            logger.info(f"Feedback received: {feedback_data}")
            
            # Check if Genie feedback API is enabled
            if not CONFIG.ENABLE_GENIE_FEEDBACK_API:
                logger.info("Genie feedback API is disabled, skipping API call")
                return
            
            # Extract the message ID and user session info
            message_id = feedback_data.get("message_id")
            user_id = feedback_data.get("user_id")
            feedback_type = feedback_data.get("feedback")
            user_session_data = feedback_data.get("user_session")
            
            if not all([message_id, user_id, feedback_type]):
                logger.error(f"Missing required feedback data: {feedback_data}")
                return
            
            # Get the user session to access conversation_id
            user_session = self.user_sessions.get(user_id)
            if not user_session or not user_session.conversation_id:
                logger.error(f"No active conversation found for user {user_id}")
                return
            
            # Convert feedback type to Genie API format
            # positive -> POSITIVE, negative -> NEGATIVE
            genie_feedback_type = "POSITIVE" if feedback_type == "positive" else "NEGATIVE"
            
            # Call the Databricks Genie send message feedback API
            logger.info(f"Sending feedback for specific message ID: {message_id} in conversation: {user_session.conversation_id}")
            await self._send_genie_feedback(
                space_id=CONFIG.DATABRICKS_SPACE_ID,
                conversation_id=user_session.conversation_id,
                message_id=message_id,
                feedback_type=genie_feedback_type
            )
            
            logger.info(f"Feedback sent successfully to Genie API for {feedback_key}")
            
        except Exception as e:
            logger.error(f"Error sending feedback to Genie API: {str(e)}")
            raise

    async def _send_genie_feedback(self, space_id: str, conversation_id: str, message_id: str, feedback_type: str):
        """Send feedback to Databricks Genie API"""
        try:
            loop = asyncio.get_running_loop()
            
            # Use the Genie API to send message feedback
            # Note: The exact method name may vary based on the API version
            # This assumes the method is called send_message_feedback
            await loop.run_in_executor(
                None,
                genie_api.send_message_feedback,
                space_id,
                conversation_id,
                message_id,
                feedback_type
            )
            
            logger.info(f"Successfully sent {feedback_type} feedback for message {message_id} in conversation {conversation_id}")
            
        except AttributeError:
            # If send_message_feedback method doesn't exist, try alternative method names
            logger.warning(f"send_message_feedback method not found, trying alternative approach")
            await self._send_genie_feedback_alternative(space_id, conversation_id, message_id, feedback_type)
        except Exception as e:
            logger.error(f"Error calling Genie API for feedback: {str(e)}")
            raise

    async def _send_genie_feedback_alternative(self, space_id: str, conversation_id: str, message_id: str, feedback_type: str):
        """Alternative method to send feedback if the direct API method is not available"""
        try:
            # If the direct API method is not available, we can use the workspace client
            # to make a direct HTTP request to the Genie feedback endpoint
            import aiohttp
            
            # Construct the API endpoint URL
            base_url = CONFIG.DATABRICKS_HOST.rstrip('/')
            api_endpoint = f"{base_url}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/feedback"
            
            # Prepare the request payload
            payload = {
                "rating": feedback_type
            }
            
            # Prepare headers
            headers = {
                "Authorization": f"Bearer {CONFIG.DATABRICKS_TOKEN}",
                "Content-Type": "application/json"
            }
            
            # Make the HTTP request
            logger.info(f"Sending feedback to: {api_endpoint}")
            logger.info(f"Payload: {payload}")
            
            async with aiohttp.ClientSession() as session:
                async with session.post(api_endpoint, json=payload, headers=headers) as response:
                    response_text = await response.text()
                    if response.status == 200:
                        logger.info(f"Successfully sent {feedback_type} feedback via HTTP API")
                    else:
                        logger.error(f"Failed to send feedback via HTTP API: {response.status} - {response_text}")
                        raise Exception(f"HTTP {response.status}: {response_text}")
                        
        except Exception as e:
            logger.error(f"Error in alternative feedback method: {str(e)}")
            raise

    async def _get_last_genie_message_id(self, conversation_id: str) -> Optional[str]:
        """Get the last message ID from the Genie conversation"""
        try:
            if not conversation_id:
                return None
                
            loop = asyncio.get_running_loop()
            # Try different method names for listing messages
            try:
                # Try list_conversation_messages first
                messages = await loop.run_in_executor(
                    None,
                    genie_api.list_conversation_messages,
                    CONFIG.DATABRICKS_SPACE_ID,
                    conversation_id,
                )
            except AttributeError:
                try:
                    # Try get_conversation_messages
                    messages = await loop.run_in_executor(
                        None,
                        genie_api.get_conversation_messages,
                        CONFIG.DATABRICKS_SPACE_ID,
                        conversation_id,
                    )
                except AttributeError:
                    # If neither method exists, return None and log a warning
                    logger.warning("No suitable method found for listing Genie conversation messages")
                    return None
            
            # Handle different response types
            if messages:
                logger.info(f"Messages response type: {type(messages)}")
                logger.info(f"Messages response attributes: {dir(messages)}")
                
                # Check if it's a response object with messages property
                if hasattr(messages, 'messages') and messages.messages:
                    logger.info(f"Found {len(messages.messages)} messages in response.messages")
                    # Sort messages by timestamp to get the most recent one
                    try:
                        sorted_messages = sorted(messages.messages, key=lambda x: getattr(x, 'created_at', 0), reverse=True)
                        if sorted_messages:
                            latest_message = sorted_messages[0]
                            logger.info(f"Latest message ID: {latest_message.message_id}")
                            return latest_message.message_id
                    except Exception as e:
                        logger.warning(f"Could not sort messages by timestamp: {e}, using last message")
                        return messages.messages[-1].message_id
                # Check if it's a list-like object
                elif hasattr(messages, '__len__') and len(messages) > 0:
                    logger.info(f"Found {len(messages)} messages in response (list-like)")
                    # Sort messages by timestamp to get the most recent one
                    try:
                        sorted_messages = sorted(messages, key=lambda x: getattr(x, 'created_at', 0), reverse=True)
                        if sorted_messages:
                            latest_message = sorted_messages[0]
                            logger.info(f"Latest message ID: {latest_message.message_id}")
                            return latest_message.message_id
                    except Exception as e:
                        logger.warning(f"Could not sort messages by timestamp: {e}, using last message")
                        return messages[-1].message_id
                # Check if it's iterable
                elif hasattr(messages, '__iter__'):
                    message_list = list(messages)
                    if message_list:
                        logger.info(f"Found {len(message_list)} messages in response (iterable)")
                        # Sort messages by timestamp to get the most recent one
                        try:
                            sorted_messages = sorted(message_list, key=lambda x: getattr(x, 'created_at', 0), reverse=True)
                            if sorted_messages:
                                latest_message = sorted_messages[0]
                                logger.info(f"Latest message ID: {latest_message.message_id}")
                                return latest_message.message_id
                        except Exception as e:
                            logger.warning(f"Could not sort messages by timestamp: {e}, using last message")
                            return message_list[-1].message_id
                else:
                    logger.warning(f"Unable to extract messages from response of type {type(messages)}")
            return None
            
        except Exception as e:
            logger.error(f"Error getting last Genie message ID: {str(e)}")
            return None

    async def _send_feedback_card(self, turn_context: TurnContext, user_session: UserSession):
        """Send a feedback card after a bot response"""
        try:
            # Check if feedback cards are enabled
            if not CONFIG.ENABLE_FEEDBACK_CARDS:
                return
                
            # Use the actual Genie message ID if available, otherwise generate a fallback
            genie_message_id = user_session.user_context.get('last_genie_message_id')
            if genie_message_id:
                message_id = genie_message_id
                logger.info(f"Creating feedback card for specific Genie message ID: {message_id}")
            else:
                # Fallback to generated ID if we don't have the Genie message ID
                message_id = f"msg_{int(datetime.now().timestamp() * 1000)}"
                logger.warning(f"No Genie message ID available for user {user_session.get_display_name()}, using fallback: {message_id}")
            
            # Create feedback card
            feedback_card = self.create_feedback_card(message_id, user_session.user_id)
            
            # Send the card as an attachment
            activity = Activity(
                type=ActivityTypes.message,
                attachments=[{
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": feedback_card
                }]
            )
            
            await turn_context.send_activity(activity)
            
        except Exception as e:
            logger.error(f"Error sending feedback card: {str(e)}")

    async def on_members_added_activity(
        self, members_added: List[ChannelAccount], turn_context: TurnContext
    ):
        ##print("Members added",members_added)
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                # Try to get user information for personalized welcome
                user_session = await self.get_or_create_user_session(turn_context)
                
                if user_session:
                    is_emulator = turn_context.activity.channel_id == "emulator"
                    welcome_message = f"""🤖 **Welcome to the Databricks Genie Bot, {user_session.name}!**

I can help you analyze your data using natural language. I'll remember our conversation context, so you can ask follow-up questions.

**👤 You're identified as:** {user_session.get_display_name()}"""

                    if is_emulator:
                        welcome_message += """

**🔧 Emulator Testing:**
You're using the Bot Emulator for testing. You can change your identity anytime using:
`/setuser your.email@company.com Your Name`"""

                    welcome_message += """

**Quick Commands:**
- `help` - Detailed bot information
- `info` - Get help getting started
- `whoami` - Your user information
- `reset` or `new chat` - Start fresh
- Just ask me anything about your data!

Ready to get started? Ask me a question!"""
                else:
                    is_emulator = turn_context.activity.channel_id == "emulator"
                    welcome_message = """🤖 **Welcome to the Databricks Genie Bot!**

I can help you analyze your data using natural language. I'll remember our conversation context, so you can ask follow-up questions.

**📧 First-time setup:**
Please provide your email address to log queries in Genie for tracking purposes."""

                    if is_emulator:
                        welcome_message += """

**🔧 Emulator Testing:**
Since you're using the Bot Emulator, please set your identity using:
`/setuser your.email@company.com Your Name`
Example: `/setuser john.doe@company.com John Doe`"""
                    else:
                        welcome_message += """

**To get started:**
- Type `email` to provide your email address
- Type `info` for help getting started"""

                    welcome_message += """

**Quick Commands:**
- `help` - Detailed bot information
- `info` - Get help getting started
- `whoami` - Your user information  
- `reset` or `new chat` - Start fresh
- Ask me anything about your data!

Ready to get started? Type `email` to begin!"""
                
                await turn_context.send_activity(welcome_message)



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("🚀 Starting Databricks Genie Bot App")

CONFIG = DefaultConfig()

if CONFIG.APP_ID and CONFIG.APP_PASSWORD:
    ADAPTER = CloudAdapter(ConfigurationBotFrameworkAuthentication(CONFIG))
else:
    SETTINGS = BotFrameworkAdapterSettings("", "")
    ADAPTER = BotFrameworkAdapter(SETTINGS)

BOT = MyBot()
is_warmed_up = False

async def warm_up_bot():
    """Run lightweight initialization to prepare the bot for first request."""
    global is_warmed_up
    if is_warmed_up:
        return
    logger.info("🔄 Warming up bot components...")
    try:
        await BOT.initialize_async() if hasattr(BOT, "initialize_async") else None
        # Optionally ping downstream dependencies here
        is_warmed_up = True
        logger.info("✅ Bot warmed up successfully.")
    except Exception as e:
        logger.error(f"❌ Warm-up failed: {e}")

async def root(req: Request) -> Response:
    """Root endpoint for Azure probe."""
    return web.Response(text="✅ Databricks Genie Bot is running")

async def health(req: Request) -> Response:
    """Health check endpoint; also ensures bot is warmed up."""
    global is_warmed_up
    if not is_warmed_up:
        logger.info("Health probe triggered - warming up bot...")
        await warm_up_bot()
    return json_response({"status": "ok", "warmed_up": is_warmed_up})

async def messages(req: Request) -> Response:
    """Main endpoint for incoming Bot Framework messages."""
    global is_warmed_up

    content_type = req.headers.get("Content-Type", "").lower()
    if "application/json" not in content_type:
        logger.error(f"Unsupported Content-Type: {content_type}")
        return Response(status=415)

    body = await req.json()
    auth_header = req.headers.get("Authorization", "")
    activity = Activity().deserialize(body)

    if not is_warmed_up:
        logger.warning("⚠️ Cold start detected — warming up before processing message...")

        try:
            async def send_warming_notice(turn_context: TurnContext):
                await turn_context.send_activity(
                    "⏳ **The Databricks Genie Bot is starting up...**\n\n"
                    "Please wait a few seconds while I get ready."
                )

            await ADAPTER.process_activity(activity, auth_header, send_warming_notice)
            logger.info("📨 Sent 'warming up' notice to Teams successfully.")
        except Exception as e:
            logger.error(f"Failed to send 'warming up' notice: {e}", exc_info=True)

        await warm_up_bot()

        try:
            logger.info("🔁 Replaying user message after warm-up...")
            await ADAPTER.process_activity(activity, auth_header, BOT.on_turn)
            logger.info("✅ Warm-up complete and user message processed.")
            return Response(status=201)
        except Exception as e:
            logger.error(f"❌ Error reprocessing message after warm-up: {e}", exc_info=True)
            return Response(status=500)
    return await process_incoming_activity(req)


async def process_incoming_activity(req: Request) -> Response:
    """Handle adapter dispatch for the incoming activity."""
    auth_header = req.headers.get("Authorization", "")

    try:
        if hasattr(ADAPTER, "process"):
            return await ADAPTER.process(req, BOT)

        body = await req.json()
        activity = Activity().deserialize(body)
        response = await ADAPTER.process_activity(activity, auth_header, BOT.on_turn)

        if isinstance(response, InvokeResponse):
            return json_response(data=response.body, status=response.status)
        return Response(status=201)
    except Exception as e:
        logger.error(f"Error processing activity: {e}", exc_info=True)
        return Response(status=500)


async def on_startup(app):
    logger.info("🌅 App startup detected — warming bot proactively...")
    await warm_up_bot()
    #await send_warming_up_message()
    

def init_func(argv=None):
    app = web.Application(middlewares=[aiohttp_error_middleware])
    app.router.add_get("/", root)
    app.router.add_get("/health", health)
    app.router.add_post("/api/messages", messages)
    app.on_startup.append(on_startup)
    return app

if __name__ == "__main__":
    app = init_func(None)
    host = "0.0.0.0"
    port = int(os.environ.get("PORT", CONFIG.PORT))
    web.run_app(app, host=host, port=port)
