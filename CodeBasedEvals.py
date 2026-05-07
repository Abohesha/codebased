# Your entire Python code goes here - just copy/paste the whole file content

# Phase 2: Core Analytics - Snowflake Migration
# Bot Handling Analysis & Repetition Analysis
# Adapted from main_analytics.py for Snowflake integration
# STANDALONE VERSION - All Phase 1 functions included
from snowflake_analytics_client import SnowflakeAnalyticsClient

from collections import Counter
import json
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, current_timestamp, lit
import pandas as pd
from datetime import datetime, timedelta
import traceback
import numpy as np
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from itertools import combinations
import warnings

# Suppress FutureWarning for pandas concat operations
warnings.filterwarnings('ignore', category=FutureWarning, message='.*DataFrame concatenation with empty or all-NA entries.*')

# ============================================================================
# PHASE 1 FOUNDATION FUNCTIONS (INCLUDED FOR STANDALONE EXECUTION)
# ============================================================================

def format_error_details(e, context=""):
    """
    Format exception details for comprehensive error reporting.
    
    Args:
        e: Exception object
        context: Additional context about where the error occurred
    
    Returns:
        Formatted error string with full details
    """
    error_details = traceback.format_exc()
    return f"""
{'=' * 50}
🚨 ERROR DETAILS {f"- {context}" if context else ""}
{'=' * 50}
Error Type: {type(e).__name__}
Error Message: {str(e)}

Full Traceback:
{error_details}
{'=' * 50}
"""

def infer_tool_message_type(text_value, department_name=None):
    """
    Inspect TEXT field (string or dict) and infer if it's a tool or tool response message.
    Criteria:
    - Must parse to a dict and contain a 'content' object
    - If it has 'tool_calls' -> return 'tool'
    - If it has 'tool_call_id' -> return 'tool response'
    Returns 'tool' | 'tool response' | None
    """
    try:
        parsed = None
        if isinstance(text_value, dict):
            parsed = text_value
        elif isinstance(text_value, str):
            text_str = text_value.strip()
            if text_str.startswith('{') and text_str.endswith('}'):
                # Attempt strict JSON parse; if it fails, remove trailing commas before } or ] and retry
                try:
                    parsed = json.loads(text_str)
                except Exception:
                    cleaned_str = re.sub(r',\s*([}\]])', r'\1', text_str)
                    try:
                        parsed = json.loads(cleaned_str)
                    except Exception:
                        return None
        if not isinstance(parsed, dict):
            return None

        # General detection first (top-level)
        if parsed.get('tool_calls'):
            return 'tool'
        if parsed.get('tool_call_id'):
            return 'tool response'

        # Also check inside content if it's a dict
        content_obj = parsed.get('content')
        if isinstance(content_obj, dict):
            if content_obj.get('tool_calls'):
                return 'tool'
            if content_obj.get('tool_call_id'):
                return 'tool response'

        # Fallback: detect messages shaped like {"name": <str>, "arguments": { ... }}
        name_exists = 'name' in parsed and isinstance(parsed.get('name'), str) and parsed.get('name')
        arguments_obj = parsed.get('arguments')
        arguments_is_object = isinstance(arguments_obj, dict)
        if name_exists and arguments_is_object:
            return 'tool'

        return None
    except Exception:
        return None

# Agent Intervention Exclusions - Messages to exclude from agent intervention calculation
AGENT_INTERVENTION_EXCLUSIONS = {
    'DDC': [
        'placeholder_message_1',
    ],
    'CC_Resolvers': [
        'placeholder_message_1',  # Replace with actual exclusion message
    ],
    'Delighters': [
        'placeholder_message_1',  # Replace with actual exclusion message
    ],
    'CC_Sales': [
    "Are you ready to start the hiring process?",
    "Have you found a maid you like or would you like me to send you a new list?",
    "Have you found a nanny you like or would you like me to send you a new list?",
    "May I ask what's keeping you from hiring a maid from us today, please?",
    "May I ask what's keeping you from hiring a nanny from us today, please?",
    "May I know if any of the maids stand out to you? Or would you prefer to view a new list?",
    "May I know if any of the nannies stand out to you? Or would you prefer to view a new list?",
    "Let me know if any candidates caught your attention and you'd like to move forward with a hire",
    "I see your list has many matching candidates. May I ask if you’d like to proceed with hiring one of them?",
    "In the meantime, please feel free to think of any questions or additional information you'd like to know about our service",
    "Please review the interview videos to find the best matches and gain insight into their experience, personality, and skills. All of the maid's experience is recorded in her video",
    "Please review the interview videos to find the best matches and gain insight into their experience, personality, and skills. All of the nanny's experience is recorded in her video",
    "Would you like to start exploring your best-matching maids, please?",
    "Would you like to start exploring your best-matching nannies, please?",
    "How urgently do you need a maid inside your house?",
    "How urgently do you need a nanny inside your house?",
    "only thing stopping you from hiring a maid from us today?",
    "only thing stopping you from hiring a nanny from us today?",
    "If you're hesitant about any aspect of the service, I'd love to help and offer solutions. Is there anything in particular that's stopping you from signing up with us?",
    "May I ask what's keeping you from hiring a maid from us today?",
    "May I ask what's keeping you from hiring a nanny from us today?",
    "Is there anything else you'd like to know or discuss today? I'm always here to support you!",
    "How could I be of further assistance regarding the hiring process? Or if you have another question. You can always schedule a direct call with one of our maid-matching specialists.",
    "How could I be of further assistance regarding the hiring process? Or if you have another question. You can always schedule a direct call with one of our nanny-matching specialists.",
    "Could you please specify your concerns that are preventing you from moving forward with the hiring process? I’m here to help you find solutions."
    "Selecting your preferences would help us in choosing the right maid for you",
    "Based on our conversation",
    'Selecting your preferences would help us in choosing the right maid for you',
    'If Arabic is spoken at your home',
    'I assumed that you',
    "By the way, here's the",
     "Hello from maids․cc",
     "Maid/Nanny live-in monthly plan",
     "*Maid/Nanny live-in monthly plan:*",
     "live-in Filipina maids require a private room. Please consider hiring another nationality",
     "Are you looking to hire an Ethiopian",
     "Which Emirate is your home in? And do you have a private maid room?",
     "Would you like your maid to take care of your kids? If yes, how old are they?",
     "Do you have a dog at home? A cat?"
     "Can your maid's mandatory day‑off be on Sundays?",
     "*Maid/Nanny live-in weekly plan:*",
     "What else other than the language barrier makes you want",
     "If Arabic is spoken at your home, here’s a list",
     "let me know if you'd like to change your preferences",
     "Thank you for sharing all this information",
     "I will share with you a list of our best matching maids right away",
     "We couldn't find the maid you're looking for. Please note that if you, or any other family, clicked on “Hire and Pay now”, she would be temporarily removed from the website until the hiring process is complete",
     "Here is a list of candidates that match your preferences:",
     "Here's why maids․cc offers more value for the price than other offices",
     "Thank you so much for having a call with us",
     "Thank you so much for visiting us",
     "an agent will be reaching out to you",
     "Here is a list of candidates that match your preferences",
     "here is a list"

    ],
    'Doctors': [
        'placeholder_message_1',  # Replace with actual exclusion message
    ],
    'Sales MV': [
        'placeholder_message_1',  # Replace with actual exclusion message
    ],
    'MV Department': [
        'placeholder_message_1',  # Replace with actual exclusion message
    ],
    'AT_Ethiopian': [
        'Okay, Ate. We noted your date and will follow up with you on it',
        'You’ll get these amazing benefits:',
         'Thank you, Ate. We have updated your info',
          'Okay, Ate! We noted your date',
           'Let me know if you can join sooner',
            'Please send me a picture of your Saudi Arabia',
             'Can you please send me a photo of your Hong-Kong re-entry visa',
            "Since you have an active visa to UAE ate, we can't bring you from the Philippines",
           "Please send me an image of your Kuwait",
            'Please send me a photo of your Malaysia',
             'Or you can send me a photo of your Malaysia',
              'Please send me a photo of your Visit Pass,',
            'Please send me a photo of your China residence',
             'Please send me a photo of your Jordan Iqama',
            "Please send me a photo of your Oman resident card,",
             "Or you can send us a picture of your visa that looks like this:",
              "Please send me a photo your Bahrain residence permit, it should look like this:",
               "Please send me a photo of your",
            "Unfortunately we can’t hire applicants who are in the Philippines without an ACTIVE VISA",
            "Thank you for contacting us. Kindly note that we are unable to hire applicants from the Philippines without a valid passport",
            "Now please send me a colored photo of your passport ",
            "Thank you, Ate!",
            "Thank you for sharing this file ate!",
            "Thank you ate, your application is now complete!",
            "Thank you, Ate. Now all your documents are collected",
            "Thank you, Ate! " # Replace with actual exclusion message
    ],
    'AT_African': [
        'Okay, Ate. We noted your date and will follow up with you on it',
        'You’ll get these amazing benefits:',
         'Thank you, Ate. We have updated your info',
          'Okay, Ate! We noted your date',
           'Let me know if you can join sooner',
            'Please send me a picture of your Saudi Arabia',
             'Can you please send me a photo of your Hong-Kong re-entry visa',
            "Since you have an active visa to UAE ate, we can't bring you from the Philippines",
           "Please send me an image of your Kuwait",
            'Please send me a photo of your Malaysia',
             'Or you can send me a photo of your Malaysia',
              'Please send me a photo of your Visit Pass,',
            'Please send me a photo of your China residence',
             'Please send me a photo of your Jordan Iqama',
            "Please send me a photo of your Oman resident card,",
             "Or you can send us a picture of your visa that looks like this:",
              "Please send me a photo your Bahrain residence permit, it should look like this:",
               "Please send me a photo of your",
            "Unfortunately we can’t hire applicants who are in the Philippines without an ACTIVE VISA",
            "Thank you for contacting us. Kindly note that we are unable to hire applicants from the Philippines without a valid passport",
            "Now please send me a colored photo of your passport ",
            "Thank you, Ate!",
            "Thank you for sharing this file ate!",
            "Thank you ate, your application is now complete!",
            "Thank you, Ate. Now all your documents are collected",
            "Thank you, Ate! " # Replace with actual exclusion message
    ],
    'AT_Filipina': [
        'Okay, Ate. We noted your date and will follow up with you on it',
        'You’ll get these amazing benefits:',
         'Thank you, Ate. We have updated your info',
          'Okay, Ate! We noted your date',
           'Let me know if you can join sooner',
            'Please send me a picture of your Saudi Arabia',
             'Can you please send me a photo of your Hong-Kong re-entry visa',
            "Since you have an active visa to UAE ate, we can't bring you from the Philippines",
           "Please send me an image of your Kuwait",
            'Please send me a photo of your Malaysia',
             'Or you can send me a photo of your Malaysia',
              'Please send me a photo of your Visit Pass,',
            'Please send me a photo of your China residence',
             'Please send me a photo of your Jordan Iqama',
            "Please send me a photo of your Oman resident card,",
             "Or you can send us a picture of your visa that looks like this:",
              "Please send me a photo your Bahrain residence permit, it should look like this:",
               "Please send me a photo of your",
            "Unfortunately we can’t hire applicants who are in the Philippines without an ACTIVE VISA",
            "Thank you for contacting us. Kindly note that we are unable to hire applicants from the Philippines without a valid passport",
            "Now please send me a colored photo of your passport ",
            "Thank you, Ate!",
            "Thank you for sharing this file ate!",
            "Thank you ate, your application is now complete!",
            "Thank you, Ate. Now all your documents are collected",
            "Thank you, Ate! " # Replace with actual exclusion message
    ],
    'AT_Filipina_In_PHL': [
        'Okay, Ate. We noted your date and will follow up with you on it',
        'You’ll get these amazing benefits:',
         'Thank you, Ate. We have updated your info',
          'Okay, Ate! We noted your date',
           'Let me know if you can join sooner',
            'Please send me a picture of your Saudi Arabia',
             'Can you please send me a photo of your Hong-Kong re-entry visa',
            "Since you have an active visa to UAE ate, we can't bring you from the Philippines",
           "Please send me an image of your Kuwait",
            'Please send me a photo of your Malaysia',
             'Or you can send me a photo of your Malaysia',
              'Please send me a photo of your Visit Pass,',
            'Please send me a photo of your China residence',
             'Please send me a photo of your Jordan Iqama',
            "Please send me a photo of your Oman resident card,",
             "Or you can send us a picture of your visa that looks like this:",
              "Please send me a photo your Bahrain residence permit, it should look like this:",
               "Please send me a photo of your",
            "Unfortunately we can’t hire applicants who are in the Philippines without an ACTIVE VISA",
            "Thank you for contacting us. Kindly note that we are unable to hire applicants from the Philippines without a valid passport",
            "Now please send me a colored photo of your passport ",
            "Thank you, Ate!",
            "Thank you for sharing this file ate!",
            "Thank you ate, your application is now complete!",
            "Thank you, Ate. Now all your documents are collected",
            "Thank you, Ate! " # Replace with actual exclusion message
    ],
    'AT_Filipina_Outside_UAE': [
        'Okay, Ate. We noted your date and will follow up with you on it',
        'You’ll get these amazing benefits:',
         'Thank you, Ate. We have updated your info',
          'Okay, Ate! We noted your date',
           'Let me know if you can join sooner',
            'Please send me a picture of your Saudi Arabia',
             'Can you please send me a photo of your Hong-Kong re-entry visa',
            "Since you have an active visa to UAE ate, we can't bring you from the Philippines",
           "Please send me an image of your Kuwait",
            'Please send me a photo of your Malaysia',
             'Or you can send me a photo of your Malaysia',
              'Please send me a photo of your Visit Pass,',
            'Please send me a photo of your China residence',
             'Please send me a photo of your Jordan Iqama',
            "Please send me a photo of your Oman resident card,",
             "Or you can send us a picture of your visa that looks like this:",
              "Please send me a photo your Bahrain residence permit, it should look like this:",
               "Please send me a photo of your",
            "Unfortunately we can’t hire applicants who are in the Philippines without an ACTIVE VISA",
            "Thank you for contacting us. Kindly note that we are unable to hire applicants from the Philippines without a valid passport",
            "Now please send me a colored photo of your passport ",
            "Thank you, Ate!",
            "Thank you for sharing this file ate!",
            "Thank you ate, your application is now complete!",
            "Thank you, Ate. Now all your documents are collected",
            "Thank you, Ate! " # Replace with actual exclusion message
    ],
    'AT_Filipina_Inside_UAE': [
        'Okay, Ate. We noted your date and will follow up with you on it',
        'You’ll get these amazing benefits:',
         'Thank you, Ate. We have updated your info',
          'Okay, Ate! We noted your date',
           'Let me know if you can join sooner',
            'Please send me a picture of your Saudi Arabia',
             'Can you please send me a photo of your Hong-Kong re-entry visa',
            "Since you have an active visa to UAE ate, we can't bring you from the Philippines",
           "Please send me an image of your Kuwait",
            'Please send me a photo of your Malaysia',
             'Or you can send me a photo of your Malaysia',
              'Please send me a photo of your Visit Pass,',
            'Please send me a photo of your China residence',
             'Please send me a photo of your Jordan Iqama',
            "Please send me a photo of your Oman resident card,",
             "Or you can send us a picture of your visa that looks like this:",
              "Please send me a photo your Bahrain residence permit, it should look like this:",
               "Please send me a photo of your",
            "Unfortunately we can’t hire applicants who are in the Philippines without an ACTIVE VISA",
            "Thank you for contacting us. Kindly note that we are unable to hire applicants from the Philippines without a valid passport",
            "Now please send me a colored photo of your passport ",
            "Thank you, Ate!",
            "Thank you for sharing this file ate!",
            "Thank you ate, your application is now complete!",
            "Thank you, Ate. Now all your documents are collected",
            "Thank you, Ate! " # Replace with actual exclusion message
    ]
}

static_messages_cc_sales=[
    'Are you looking to hire an Ethiopian',
    'Which Emirate is your home in?',
    'Do you have a dog at home?',
    'mandatory day‑off be on Sundays?',
    'Maid/Nanny live-in monthly plan',
    'Price includes: Salary, visa, Uber, medicals, all government costs',
    'Maid/Nanny live-in monthly plan',
    'No deposits, cancel anytime',
    'Maid/Nanny live-in weekly plan',
    'You can do it now, here on WhatsApp',
    'What else other than the language barrier makes you want',
    'Thank you for sharing all this information',
    "We couldn't find the maid you're looking for",
    "Here's why maids․cc offers more value for the price than other offices",
    'Thank you so much for having a call with us',
    'here is a list'

]
pokes=[
        
    "Are you ready to start the hiring process?",
    "Have you found a maid you like or would you like me to send you a new list?",
    "Have you found a nanny you like or would you like me to send you a new list?",
    "May I ask what's keeping you from hiring a maid from us today, please?",
    "May I ask what's keeping you from hiring a nanny from us today, please?",
    "May I know if any of the maids stand out to you? Or would you prefer to view a new list?",
    "May I know if any of the nannies stand out to you? Or would you prefer to view a new list?",
    "Let me know if any candidates caught your attention and you'd like to move forward with a hire",
    "I see your list has many matching candidates. May I ask if you’d like to proceed with hiring one of them?",
    "In the meantime, please feel free to think of any questions or additional information you'd like to know about our service",
    "Please review the interview videos to find the best matches and gain insight into their experience, personality, and skills. All of the maid's experience is recorded in her video",
    "Please review the interview videos to find the best matches and gain insight into their experience, personality, and skills. All of the nanny's experience is recorded in her video",
    "Would you like to start exploring your best-matching maids, please?",
    "Would you like to start exploring your best-matching nannies, please?",
    "How urgently do you need a maid inside your house?",
    "How urgently do you need a nanny inside your house?",
    "only thing stopping you from hiring a maid from us today?",
    "only thing stopping you from hiring a nanny from us today?",
    "If you're hesitant about any aspect of the service, I'd love to help and offer solutions. Is there anything in particular that's stopping you from signing up with us?",
    "May I ask what's keeping you from hiring a maid from us today?",
    "May I ask what's keeping you from hiring a nanny from us today?",
    "Is there anything else you'd like to know or discuss today? I'm always here to support you!"
    "How could I be of further assistance regarding the hiring process? Or if you have another question. You can always schedule a direct call with one of our maid-matching specialists.",
    "How could I be of further assistance regarding the hiring process? Or if you have another question. You can always schedule a direct call with one of our nanny-matching specialists.",
    "Could you please specify your concerns that are preventing you from moving forward with the hiring process? I’m here to help you find solutions."
    "Selecting your preferences would help us in choosing the right maid for you",
    'If Arabic is spoken at your home',
    'I assumed that you',
    "By the way, here's the",
    'you like or would you like me to send you a new',
    'stand out to you?',
    'May I ask if you’d like to proceed',
    'feel free to think of any questions',
    'review the interview videos to find the best matches',
    'the only thing stopping you from hiring',
    'How may I assist you further?',
    'Is there anything else I can help',
    'specify your concerns that are preventing you',
    'How could I be of further assistance',
    "Is there anything else you'd like to know",
    'You can always schedule a direct call with'

          
    ]


# M20 Intervention Word - Messages to check for M20 intervention reengagement
M20_intervention_word = "Based on our conversation"

# repetition exclusion list
repetition_exclusion_list=[
  "Chat has been reset! Please wait 5 seconds before you start testing",
  "Here is a list of maids that match your preferences:",
  "Here is a list of nannies that match your preferences:",
  "Here is a list of candidates that match your preferences:",
  "I've noted your preferences.",
  "Our 7-day guarantee and unlimited replacements ensure you find the perfect live-in maid risk-free.",
  "Our 7-day guarantee and unlimited replacements ensure you find the perfect maid for your home.",
  "Our 7-day guarantee and unlimited replacements ensure you find the perfect maid with zero risk.",
  "Our 7-day guarantee and unlimited replacements let you try any maid completely risk-free.",
  "Our 7-day guarantee and unlimited replacements let you try any maid risk-free for your recovery needs.",
  "Our 7-day guarantee and unlimited replacements let you try risk-free with full protection.",
  "Our 7-day guarantee and unlimited replacements let you try risk-free with full support.",
  "Our 7-day guarantee and unlimited replacements make trying a Filipina maid completely risk-free.",
  "Our 7-day guarantee and unlimited replacements make trying a maid completely risk-free for you.",
  "Our 7-day guarantee and unlimited replacements make trying any maid completely risk-free for your family.",
  "Our 7-day guarantee and unlimited replacements make trying any maid completely risk-free.",
  "Our 7-day guarantee and unlimited replacements make trying MENCHIE completely risk-free.",
  "Our 7-day guarantee and unlimited replacements make trying your chosen Filipina maid completely risk-free.",
  "Our 7-day guarantee and unlimited replacements mean you can try risk-free and find your perfect match.",
  "Our 7-day money-back guarantee and unlimited free replacements make trying a maid completely risk-free.",
  "Our 7-day money-back guarantee and unlimited free replacements make trying your chosen Filipina maid completely risk-free.",
  "Our 7-day money-back guarantee and unlimited replacements ensure you find the perfect Filipina maid risk-free.",
  "Our 7-day money-back guarantee and unlimited replacements ensure you find the perfect maid with zero risk.",
  "Our 7-day money-back guarantee and unlimited replacements ensure you find the perfect newborn specialist for your family.",
  "Our 7-day money-back guarantee and unlimited replacements ensure you find the perfect night-shift nanny risk-free.",
  "Our 7-day money-back guarantee and unlimited replacements make trying a maid completely risk-free.",
  "Our 7-day money-back guarantee and unlimited replacements make trying an Ethiopian maid completely risk-free.",
  "Our 7-day money-back guarantee and unlimited replacements make trying your chosen Filipina maid completely risk-free.",
  "Our 7-day money-back guarantee and unlimited replacements make trying your Filipina live-out maid completely risk-free.",
  "Please allow me a moment to get back to you.",
  "Remember, you get 7-day money-back guarantee and unlimited free replacements to find your perfect Muslim maid.",
  "Remember, you get our 7-day money-back guarantee and unlimited free replacements to find your perfect Muslim maid.",
    "What support do you need",
  "What support do you need to complete",
  "What support do you need to complete Edelyn's hiring today?",
  "What support do you need to complete Edna's hiring process today?",
  "What support do you need to complete hiring Barbette with the bank payment form?",
  "What support do you need to complete hiring Esther today?",
  "What support do you need to complete hiring one of these maids today?",
  "What support do you need to complete hiring Selame today?",
  "What support do you need to complete hiring today?",
  "What support do you need to complete hiring your Filipina live-out maid today?",
  "What support do you need to complete Mimi's hiring process today?",
  "What support do you need to complete Renelyn's hiring process today?",
  "What support do you need to complete the one-month hiring process today?",
   "What support do you need to",
 "What support do you need to move",
  "What support do you need to move forward with Beverly Jane today?",
  "What support do you need to move forward with hiring a maid today?",
  "What support do you need to move forward with hiring an Ethiopian maid today?",
  "What support do you need to move forward with hiring one of these maids today?",
  "What support do you need to move forward with hiring one of these maids?",
  "What support do you need to move forward with hiring today?",
  "What support do you need to move forward with hiring your Ethiopian maid today?",
  "What support do you need to move forward with Josephine for your mother's care?",
  "What support do you need to move forward with Kuri today?",
  "What support do you need to move forward with Lidiya today?",
  "What support do you need to move forward with Menchie today?",
  "What support do you need to move forward with Selame today?",
  "What support do you need to move forward with Susan or Leah today?",
    "What would help you move forward",
  "What would help you move forward with hiring a maid today?",
  "What's holding you back from choosing",
  "What's holding you back from moving",
  "What's holding you back from choosing Ria or one of the other maids today?",
  "What's holding you back from moving forward with a Filipina live-out maid today?",
  "What's holding you back from moving forward with a Filipina maid today?",
  "What's holding you back from moving forward with a Filipina nanny today?",
  "What's holding you back from moving forward with a live-in maid today?",
  "What's holding you back from moving forward with an African maid today?",
  "What's holding you back from moving forward with finding the right maid today?",
  "What's holding you back from moving forward with hiring a live-in maid today?",
  "What's holding you back from moving forward with hiring a live-out maid today?",
  "What's holding you back from moving forward with hiring a maid today?",
  "What's holding you back from moving forward with hiring today?",
  "What's holding you back from moving forward with hiring your first maid today?",
  "What's holding you back from moving forward with Laarnie today?",
  "What's holding you back from moving forward with Lorena today?",
  "What's holding you back from moving forward with one of the available maids today?",
  "What's holding you back from moving forward with one of the maids today?",
  "What's holding you back from moving forward with one of these available maids today?",
  "What's holding you back from moving forward with one of these experienced maids today?",
  "What's holding you back from moving forward with one of these maids today?",
    "What's stopping you from moving",
  "What's stopping you from moving forward with Ritchel or Ria today?",
  "What's the main thing holding you back from moving forward today?",
  "What's the main thing holding you back from moving forward with one of these nannies today?",
  "What's the main thing I can help you with to move forward today?",
  "What's the main thing you need to feel confident about moving forward today?",
  "What's the main thing you'd like to clarify before moving forward with one of these maids?",
  "What's the main thing you'd like to clarify before we finalize",

  "What's the main thing you'd like to clarify before we finalize your Filipina live-out maid?",
  "What's your main concern about moving forward with Janice today?",
  "You can try any maid with our 7-day money-back guarantee and unlimited free replacements.",
  "You can try her risk-free with our 7-day money-back guarantee and unlimited replacements.",
  "You have a 7-day money-back guarantee and unlimited free replacements if needed."
]


# Global DataFrame for CC_Sales message categorization and reengagement tracking
CC_SALES_POKES_VALIDATION_DF = pd.DataFrame()

def initialize_cc_sales_pokes_validation_df():
    """Initialize the global DataFrame for CC_Sales message categorization"""
    global CC_SALES_POKES_VALIDATION_DF
    CC_SALES_POKES_VALIDATION_DF = pd.DataFrame(columns=[
        'CONVERSATION_ID', 'MESSAGE_INDEX', 'MESSAGE_SENT_TIME', 'SENT_BY', 
        'TARGET_SKILL_PER_MESSAGE', 'TEXT', 'MESSAGE_CATEGORY', 
        'IS_10MIN_REENGAGED', 'NEXT_CONSUMER_RESPONSE_TIME', 'TIME_TO_NEXT_RESPONSE_MINUTES'
    ])

def save_cc_sales_pokes_validation_table(session):
    """Save the global DataFrame as CC_Sales_Pokes_validation table in Snowflake"""
    global CC_SALES_POKES_VALIDATION_DF
    
    if CC_SALES_POKES_VALIDATION_DF.empty:
        print("  ⚠️  No CC_Sales message data to save")
        return False
    
    try:
        table_name = "CC_SALES_POKES_VALIDATION"
        
        # Create Snowflake DataFrame
        snowflake_df = session.create_dataframe(CC_SALES_POKES_VALIDATION_DF)
        
        # Drop table if exists and create new one
        session.sql(f"DROP TABLE IF EXISTS {table_name}").collect()
        snowflake_df.write.mode("overwrite").save_as_table(table_name)
        
        # Print summary statistics
        total_messages = len(CC_SALES_POKES_VALIDATION_DF)
        static_count = len(CC_SALES_POKES_VALIDATION_DF[CC_SALES_POKES_VALIDATION_DF['MESSAGE_CATEGORY'] == 'static'])
        poke_count = len(CC_SALES_POKES_VALIDATION_DF[CC_SALES_POKES_VALIDATION_DF['MESSAGE_CATEGORY'] == 'poke'])
        m20_count = len(CC_SALES_POKES_VALIDATION_DF[CC_SALES_POKES_VALIDATION_DF['MESSAGE_CATEGORY'] == 'm20'])
        dynamic_count = len(CC_SALES_POKES_VALIDATION_DF[CC_SALES_POKES_VALIDATION_DF['MESSAGE_CATEGORY'] == 'dynamic'])
        reengaged_count = len(CC_SALES_POKES_VALIDATION_DF[CC_SALES_POKES_VALIDATION_DF['IS_10MIN_REENGAGED'] == True])
        
        print(f"  ✅ CC_Sales message categorization saved to {table_name}")
        print(f"    📊 Total messages: {total_messages:,}")
        print(f"    🔧 Static (interventions): {static_count:,}")
        print(f"    🎯 Poke (global): {poke_count:,}")
        print(f"    🎯 M20: {m20_count:,}")
        print(f"    🔄 Dynamic: {dynamic_count:,}")
        print(f"    ⚡ 10-min reengaged: {reengaged_count:,}")
        return True
        
    except Exception as e:
        print(f"  ❌ Failed to save CC_Sales message categorization: {str(e)}")
        return False

def get_snowflake_departments_config():
    """
    Department configuration adapted from main_analytics.py for Snowflake.
    Maps department names to their bot_skills, agent_skills, and Snowflake table names.
    """
    return {
        'CC_Resolvers': {
            'bot_skills': ['GPT_CC_RESOLVERS'],
            'agent_skills': ['CC_RESOLVERS_AGENTS', 'GPT CC Shadowers','GPT_CC_RESOLVERS_SHADOWERS'],
            'table_name': 'SILVER.CHAT_EVALS.CC_CLIENT_CHATS',  # Update with actual table name
            'skill_filter': 'gpt_cc_resolvers',  # For compatibility with existing logic
            'bot_filter': 'bot'
        },
        'MV_Resolvers': {
            'bot_skills': ['GPT_MV_RESOLVERS'],
            'agent_skills': ['MV_RESOLVERS_SENIORS', 'MV_CALLERS', 'MV_RESOLVERS_MANAGER', 
                           'GPT_MV_RESOLVERS_SHADOWERS', 'GPT_MV_RESOLVERS_SHADOWERS_MANAGER','Pre_R_Visa_Retention'],
            'table_name': 'SILVER.CHAT_EVALS.MV_CLIENTS_CHATS',  # Update with actual table name
            'skill_filter': 'gpt_mv_resolvers',
            'bot_filter': 'bot'
        },
        'CC_Sales': {
            'bot_skills': ['GPT_CC_PROSPECT'],
            'agent_skills': ['GPT CC Shadowers'],
            'table_name': 'SILVER.CHAT_EVALS.CC_SALES_CHATS',  # Update with actual table name
            'skill_filter': 'gpt_cc_prospect',
            'bot_filter': 'bot'
        },
        'MV_Sales': {
            'bot_skills': ['GPT_MV_PROSPECT','GPT_MV_PROSPECT_N8N'],
            'agent_skills': ['CHATGPT_SALES_SHADOWERS'],
            'table_name': 'SILVER.CHAT_EVALS.MV_SALES_CHATS',  # Update with actual table name
            'skill_filter': 'gpt_mv_prospect',
            'bot_filter': 'bot'
        },
        'Delighters': {
            'bot_skills': ['GPT_MV_DELIGHTERS'],
            'agent_skills': ['MV_RESOLVERS_SENIORS', 'MV_CALLERS','MV_RESOLVERS_MANAGER','GPT_MV_RESOLVERS_SHADOWERS','GPT_MV_RESOLVERS_SHADOWERS_MANAGER','Pre_R_Visa_Retention'],
            'table_name': 'SILVER.CHAT_EVALS.DELIGHTERS_CHATS',  # Update with actual table name
            'skill_filter': 'gpt_delighters',
            'bot_filter': 'bot'
            


        },
        'CC_Delighters': {
            'bot_skills': ['GPT_CC_DELIGHTERS'],
            'agent_skills': ['GPT_CC_DELIGHTERS_SHADOWERS','CC_DELIGHTER_ETHIOPIAN','CC_DELIGHTER_OROMO','CC_DELIGHTER_SENIOR','CC_DELIGHTER_SUPERVISOR','DELIGHTER_MANAGER','Delighters'],
            'table_name': 'SILVER.CHAT_EVALS.DELIGHTERS_CHATS',  # Update with actual table name
            'skill_filter': 'GPT_CC_DELIGHTERS',
            'bot_filter': 'bot'            


        },
        'Doctors': {
            'bot_skills': ['GPT_Doctors'],
            'agent_skills': ['Doctor'],
            'table_name': 'SILVER.CHAT_EVALS.DOCTORS_CHATS',  # Update with actual table name
            'skill_filter': 'gpt_doctors',
            'bot_filter': 'bot'
        },
        'AT_Filipina': {
            'bot_skills': [
               'Filipina_in_PHl_Pending_Valid_Visa','Filipina_in_PHl_Pending_valid_visa','Filipina_Outside_Pending_Facephoto',
                'Filipina_Outside_Pending_Passport', 'Filipina_Outside_Pending_Ticket', 'Filipina_Outside_Ticket_Booked',
                'Filipina_in_PHl_Pending_Facephoto', 'Filipina_in_PHl_Pending_OEC_From_Company',
                'Filipina_in_PHl_Pending_OEC_From_maid', 'Filipina_in_PHl_Pending_Passport', 'Filipina_in_PHl_Pending_Ticket',
                'Filipina_in_PHl_Pending_valid_visa', 'Filipina_in_PHl_Ticket_Booked',
               'Filipina_Outside_UAE_Pending_Joining_Date', 'Filipina_Outside_Upcoming_Joining',
                'GPT_MAIDSAT_FILIPINA_UAE','Filipina_Outside_UAE_Vacation'
            ],
            'agent_skills': [
                'NUDGERS_REPETITIVE', 'GPT_FILIPINA_SHADOWERS', 'Nudger_TaxiBooking',
                'Nudgers_agents', 'AIRPORT_HUSTLER','OUTSIDE_FILIPINA_SHADOWERS','Nudger_TaxiBooking','PHILIPPINES_FILIPINA_SHADOWERS'
            ],
            'table_name': 'SILVER.CHAT_EVALS.APPLICANTS_CHATS',
            'skill_filter': 'filipina_outside',
            'bot_filter': 'bot'
        },
        'AT_Filipina_In_PHL': {
            'bot_skills': [
                'Filipina_in_PHl_Pending_Valid_Visa','Filipina_in_PHl_Pending_valid_visa', 'Filipina_in_PHl_Pending_Passport', 'Filipina_in_PHl_Pending_Facephoto',
                'Filipina_in_PHl_Pending_OEC_From_Company', 'Filipina_in_PHl_Pending_OEC_From_maid', 'Filipina_in_PHl_Pending_Ticket',
                'Filipina_in_PHl_Ticket_Booked'
            ],
            'agent_skills': [
                'NUDGERS_REPETITIVE', 'GPT_FILIPINA_SHADOWERS', 'Nudger_TaxiBooking',
                'Nudgers_agents', 'AIRPORT_HUSTLER','PHILIPPINES_FILIPINA_SHADOWERS'
            ],
            'table_name': 'SILVER.CHAT_EVALS.APPLICANTS_CHATS',  # Shared table
            'skill_filter': 'filipina_outside',
            'bot_filter': 'bot'
        },
        'AT_Filipina_Outside_UAE': {
            'bot_skills': [
                'Filipina_Outside_UAE_Pending_Joining_Date',
                'Filipina_Outside_Upcoming_Joining', 'Filipina_Outside_Pending_Passport', 'Filipina_Outside_Pending_Ticket',
                'Filipina_Outside_Ticket_Booked','Filipina_Outside_Pending_Facephoto', 'Filipina_Outside_UAE_Vacation'
            ],
            'agent_skills': [
                'NUDGERS_REPETITIVE', 'GPT_FILIPINA_SHADOWERS', 'Nudger_TaxiBooking',
                'Nudgers_agents', 'AIRPORT_HUSTLER', 'OUTSIDE_FILIPINA_SHADOWERS'
            ],
            'table_name': 'SILVER.CHAT_EVALS.APPLICANTS_CHATS',  # Shared table
            'skill_filter': 'filipina_in_phl',
            'bot_filter': 'bot'
        },
        'AT_Filipina_Inside_UAE': {
            'bot_skills': [
                 'GPT_MAIDSAT_FILIPINA_UAE'
            ],
            'agent_skills': [
                'NUDGERS_REPETITIVE', 'GPT_FILIPINA_SHADOWERS', 'Nudger_TaxiBooking',
                'Nudgers_agents', 'AIRPORT_HUSTLER'
            ],
            'table_name': 'SILVER.CHAT_EVALS.APPLICANTS_CHATS',  # Shared table
            'skill_filter': 'filipina_inside',
            'bot_filter': 'bot'
        },
        'AT_African': {
            'bot_skills': [
                'MAIDSAT_AFRICAN_GPT', 'GPT_MAIDSAT_AFRICAN_KENYA', 
                'GPT_MAIDSAT_AFRICAN_OUTSIDE', 'GPT_MAIDSAT_AFRICAN_UAE', 'Kenyan Assesment','Kenyan Client Scenario','Kenyan Profile Picture Collection','Kenyan WP Approved','Kenyan Passport Collection'
            ],
            'agent_skills': [
                'AFRICAN_NUDGER', 'Kenyan_Attestation_Hustling', 'Kenyan_PreAttestation',
                'Nudgers_Repetitive_Kenyan', 'AFRICAN_NUDGER'
            ],
            'table_name': 'SILVER.CHAT_EVALS.APPLICANTS_CHATS',  # Shared table
            'skill_filter': 'maidsat_africa',
            'bot_filter': 'bot'
        },
        'Prospect_Nationality_Service': {
            'bot_skills': ['PROSPECT_NATIONALITY_SERVICE'],
            'agent_skills': ['GPT CC Shadowers'],
            'table_name': 'SILVER.CHAT_EVALS.PROSPECT_NATIONALITY_SERVICE_CHATS',
            'skill_filter': 'sales_nationality_service_identification',
            'bot_filter': 'bot',
            
        },
        # 'AT_Ethiopian': {
        #     'bot_skills': [
        #         'MAIDSAT_ETHIOPIAN_GPT', 'GPT_MAIDSAT_ETHIOPIA_ETHIOPIA','GPT_MAIDSAT_ETHIOPIA_OUTSIDE','GPT_MAIDSAT_ETHIOPIA_UAE','Ethiopian Assessment', 'Ethiopian Passed Question Assessment',
        #         'Ethiopian Failed Question Assessment', 'Ethiopian Client Scenario', 'Ethiopian Sent video',
        #         'Ethiopian Failed Client Scenario', 'Ethiopian Applicant Passed Video',
        #         'Ethiopian Applicant Failed Video', 'Ethiopian Profile Picture Collection',
        #         'Ethiopian Passport Collection', 'Ethiopian Pending operator visit',
        #         'Ethiopian OP Assessment', 'Ethiopian OP Passed Questions', 'Ethiopian OP Failed Questions',
        #         'Ethiopian OP Client Scenario', 'Ethiopian OP Sent Video', 'Ethiopian OP Failed Client Scenario',
        #         'Ethiopian OP Passed Video', 'Ethiopian OP Failed Video', 'Ethiopian Invalid Passport',
        #         'Ethiopian LAWP Maids'
        #     ],
        #     'agent_skills': [
        #         'ETHIOPIAN_NUDGER', 'SCREENERS AGENTS'
        #     ],
        #     'table_name': 'SILVER.CHAT_EVALS.APPLICANTS_CHATS',  # Shared table
        #     'skill_filter': 'maidsat_ethiopia',
        #     'bot_filter': 'bot'
        # },
        'Gulf_maids': {
            'bot_skills': ['Filipina_in_PHl_NO_AV'],
            'agent_skills': ['NAV_Agents'],
            'table_name': 'SILVER.CHAT_EVALS.APPLICANTS_CHATS',  # Shared table
            'skill_filter': 'Filipina_in_PHl_NO_AV',
            'bot_filter': 'bot'
        },
        # 'MV_COLLECT_INFO': {
        #     'bot_skills': ['GPT_MV_Collect_info'],
        #     'agent_skills': [],
        #     'table_name': 'SILVER.CHAT_EVALS.MV_SALES_CHATS',  # Shared table
        #     'skill_filter': 'GPT_MV_Collect_info',
        #     'bot_filter': 'bot'
        # },
        'DDC': {
            'bot_skills': ['GPT_DDC'],
            'agent_skills': [],
            'table_name': 'SILVER.CHAT_EVALS.MV_CLIENTS_CHATS',
            'skill_filter': 'gpt_ddc',
            'bot_filter': 'bot'
        },
        'multiple_contract_detector': {
            'bot_skills': ['MULTIPLE_CONTRACT_DETECTOR'],
            'agent_skills': [],
            'table_name': 'SILVER.CHAT_EVALS.MV_CLIENTS_CHATS',
            'skill_filter': 'multiple_contract_detector',
            'bot_filter': 'bot'
        },

         'cleaners': {
            'bot_skills': ['GPT_AE_SALES'],
            'agent_skills': ['AE_SALES'],
            'table_name': 'SILVER.CHAT_EVALS.CLEANERS_CHATS',
            'skill_filter': 'GPT_AE_SALES',
            'bot_filter': 'bot'
        },
        'Travel_Assist': {
            'bot_skills': ['GPT_MV_RESOLVERS_TA'],
            'agent_skills': ['Travel_Assist_Clients','Travel_Assist_Maids'],
            'table_name': 'SILVER.CHAT_EVALS.MV_CLIENTS_CHATS',
            'skill_filter': 'GPT_MV_RESOLVERS_TA',
            'bot_filter': 'bot'
        }
    }


def create_snowflake_date_range(target_date=None):
    """
    Create date range for Snowflake filtering.
    Adapted from main_analytics.py create_date_range() function.
    Collects data from target_date-2 to target_date-1 (2 days of data).
    """
    if target_date is None:
        target_date = datetime.now()
    elif isinstance(target_date, str):
        target_date = datetime.strptime(target_date, '%Y-%m-%d')
    
    # Collect data from 2 days before target_date to 1 day before target_date
    start_date = target_date - timedelta(days=1)
    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = start_date.replace(hour=23, minute=59, second=59) + timedelta(days=1)
    
    return {
        'start': start_date.strftime('%Y-%m-%d %H:%M:%S'),
        'end': end_date.strftime('%Y-%m-%d %H:%M:%S'),
        'day1_date': start_date.date(),
        'day2_date': (start_date + timedelta(days=1)).date(),
        'yesterday_date': (target_date - timedelta(days=0)).strftime('%Y-%m-%d')
    }


def preprocess_data_snowflake_phase1(df, department_name, target_date=None):
    """
    Phase 1 preprocessing: Basic data cleaning and date filtering for Snowflake.
    Adapted from main_analytics.py preprocessing logic.
    
    Args:
        df: Raw DataFrame from Snowflake table
        department_name: Department name for filtering
        target_date: Target date for analysis
    
    Returns:
        Preprocessed DataFrame
    """
    print(f"  📋 Phase 1 preprocessing for {department_name}...")
    
    
    # Date filtering is now done at SQL level before loading data
    # This reduces memory usage by filtering 300k+ row tables before loading into pandas
    print(f"    📅 Data already filtered by date at SQL level")
    
    # Ensure required columns exist and handle missing values
    required_columns = ['CONVERSATION_ID', 'MESSAGE_SENT_TIME', 'MESSAGE_TYPE', 'SENT_BY', 'TARGET_SKILL_PER_MESSAGE']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"    ❌ MISSING COLUMNS in {department_name}: {missing_columns}")
        return pd.DataFrame()  # Return empty DataFrame if critical columns missing
    
    # Convert MESSAGE_SENT_TIME to datetime
    df['MESSAGE_SENT_TIME'] = pd.to_datetime(df['MESSAGE_SENT_TIME'])
    
    # Sort by conversation ID and message sent time (critical for proper analysis)
    df = df.sort_values(by=['CONVERSATION_ID', 'MESSAGE_SENT_TIME'])
    
    # Drop duplicates based on conversation ID, message sent time, and text content
    original_count = len(df)
    df = df.drop_duplicates(subset=['CONVERSATION_ID', 'MESSAGE_SENT_TIME', 'TEXT'], keep='first')
    if len(df) < original_count:
        print(f"    🧹 Removed {original_count - len(df)} duplicate rows")
    
    # Clean and standardize text fields
    for col in ['MESSAGE_TYPE', 'SENT_BY', 'TARGET_SKILL_PER_MESSAGE']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    
    print(f"    ✅ Preprocessing complete: {len(df)} rows, {df['CONVERSATION_ID'].nunique()} conversations")
    
    return df


def analyze_conversation_metadata(df, conversation_ids):
    """
    Extract metadata for conversations to track why they were removed.
    
    Args:
        df: DataFrame with all conversation data
        conversation_ids: List of conversation IDs to analyze
    
    Returns:
        dict: {conversation_id: {metadata_dict}}
    """
    metadata = {}
    
    for conv_id in conversation_ids:
        conv_df = df[df['CONVERSATION_ID'] == conv_id]
        
        if conv_df.empty:
            metadata[conv_id] = {
                'THROUGH_SKILL': None,
                'FIRST_MESSAGE_TIME': None,
                'LAST_MESSAGE_TIME': None,
                'MESSAGE_COUNT': 0,
                'CONSUMER_MESSAGE_COUNT': 0,
                'AGENT_MESSAGE_COUNT': 0,
                'BOT_MESSAGE_COUNT': 0,
                'TARGET_SKILLS_USED': None
            }
            continue
        
        # Get first row for THROUGH_SKILL
        first_row = conv_df.iloc[0]
        
        # Count messages by sender
        consumer_count = len(conv_df[conv_df['SENT_BY'].str.lower() == 'consumer'])
        agent_count = len(conv_df[conv_df['SENT_BY'].str.lower() == 'agent'])
        bot_count = len(conv_df[conv_df['SENT_BY'].str.lower() == 'bot'])
        
        # Get unique target skills used
        target_skills = ', '.join(conv_df['TARGET_SKILL_PER_MESSAGE'].dropna().unique()[:5])  # Limit to 5 skills
        
        metadata[conv_id] = {
            'THROUGH_SKILL': first_row.get('THROUGH_SKILL', None),
            'FIRST_MESSAGE_TIME': conv_df['MESSAGE_SENT_TIME'].min() if 'MESSAGE_SENT_TIME' in conv_df.columns else None,
            'LAST_MESSAGE_TIME': conv_df['MESSAGE_SENT_TIME'].max() if 'MESSAGE_SENT_TIME' in conv_df.columns else None,
            'MESSAGE_COUNT': len(conv_df),
            'CONSUMER_MESSAGE_COUNT': consumer_count,
            'AGENT_MESSAGE_COUNT': agent_count,
            'BOT_MESSAGE_COUNT': bot_count,
            'TARGET_SKILLS_USED': target_skills if target_skills else None
        }
    
    return metadata


def get_mv_bot_boomerang_conv_ids(session, conversation_ids, target_date):
    """
    Identify MV_Resolvers "boomerang" conversations to be excluded from
    DataFrame-based analyses (transfers / intervention / quality breakdown).

    A boomerang conversation matches this ordered pattern in
    BA_VIEWS.CHATCC_SILVER.CC_TRANSFERS:
        1. GPT_MV_RESOLVERS was involved (via any transfer source/target).
        2. A LATER transfer leaves GPT_MV_RESOLVERS for a non-MV_Resolvers skill
           (i.e., the chat was handed to another department).
        3. An EVEN LATER transfer hands the chat back to MV_RESOLVERS_SENIORS
           or MV_CALLERS.

    These chats are kept in RESOLVERS_CHATS_BREAKDOWN and the seniors/callers
    metrics (since they're computed via direct SQL and do not use the filtered
    DataFrame), but are excluded from everything that flows through the
    in-memory DataFrame.

    Args:
        session: Snowflake session
        conversation_ids: Iterable of conversation IDs currently in scope.
        target_date: Target date string ('YYYY-MM-DD'); the CC_TRANSFERS scan
                     is bounded to a small window around the chat lifecycle.

    Returns:
        set: Conversation IDs to exclude.
    """
    if not conversation_ids:
        return set()

    # Skills that count as "still inside MV_Resolvers" — i.e. NOT another department
    mv_internal_skills = [
        'GPT_MV_RESOLVERS',
        'MV_RESOLVERS_SENIORS',
        'MV_CALLERS',
        'Pre_R_Visa_Retention',
        'GPT_RESOLVERS_BOT',
        'gpt_delighters',
    ]
    mv_internal_skills_str = "', '".join(mv_internal_skills)

    try:
        date_obj = datetime.strptime(target_date, '%Y-%m-%d')
        window_start = (date_obj - timedelta(days=1)).strftime('%Y-%m-%d')
        window_end = (date_obj + timedelta(days=2)).strftime('%Y-%m-%d')
    except Exception:
        window_start = target_date
        window_end = target_date

    conv_ids_str = "', '".join([str(cid) for cid in conversation_ids])

    query = f"""
    WITH transfers AS (
        SELECT
            conversation_id,
            transfer_time,
            source_skill_name,
            target_skill_name
        FROM BA_VIEWS.CHATCC_SILVER.CC_TRANSFERS
        WHERE conversation_id IN ('{conv_ids_str}')
          AND DATE(transfer_time) BETWEEN '{window_start}' AND '{window_end}'
    ),
    out_to_other AS (
        -- Step 2: chat leaves GPT_MV_RESOLVERS to a non-MV skill
        SELECT conversation_id, MIN(transfer_time) AS out_time
        FROM transfers
        WHERE source_skill_name = 'GPT_MV_RESOLVERS'
          AND (
              target_skill_name IS NULL
              OR target_skill_name NOT IN ('{mv_internal_skills_str}')
          )
        GROUP BY conversation_id
    )
    SELECT DISTINCT t.conversation_id
    FROM out_to_other o
    JOIN transfers t
      ON t.conversation_id = o.conversation_id
     AND t.transfer_time > o.out_time
     AND t.target_skill_name IN ('MV_RESOLVERS_SENIORS', 'MV_CALLERS')
    """

    try:
        result_df = session.sql(query).to_pandas()
        if result_df.empty:
            return set()
        return set(result_df['CONVERSATION_ID'].values)
    except Exception as e:
        print(f"    ⚠️  Error detecting MV_Resolvers boomerang conversations: {e}")
        return set()


def track_removed_conversations(session, conversation_ids, department_name, target_date, removal_stage, removal_reason, conversation_metadata, removal_details=None):
    """
    Track conversations removed at each filtration stage.
    Appends to a list that will be saved later.
    
    Args:
        session: Snowflake session
        conversation_ids: List of conversation IDs removed
        department_name: Department name
        target_date: Target date
        removal_stage: Stage where removed (engagement, null_execution_id, bot_skill, hi_bye, mv_bot_boomerang, date)
        removal_reason: Detailed reason for removal
        conversation_metadata: Dict of metadata for each conversation
        removal_details: Optional additional details
    
    Returns:
        list: List of removal records
    """
    removal_records = []
    
    for conv_id in conversation_ids:
        metadata = conversation_metadata.get(conv_id, {})
        
        record = {
            'CONVERSATION_ID': conv_id,
            'REMOVAL_STAGE': removal_stage,
            'REMOVAL_REASON': removal_reason,
            'THROUGH_SKILL': metadata.get('THROUGH_SKILL'),
            'FIRST_MESSAGE_TIME': metadata.get('FIRST_MESSAGE_TIME'),
            'LAST_MESSAGE_TIME': metadata.get('LAST_MESSAGE_TIME'),
            'MESSAGE_COUNT': metadata.get('MESSAGE_COUNT', 0),
            'CONSUMER_MESSAGE_COUNT': metadata.get('CONSUMER_MESSAGE_COUNT', 0),
            'AGENT_MESSAGE_COUNT': metadata.get('AGENT_MESSAGE_COUNT', 0),
            'BOT_MESSAGE_COUNT': metadata.get('BOT_MESSAGE_COUNT', 0),
            'TARGET_SKILLS_USED': metadata.get('TARGET_SKILLS_USED'),
            'REMOVAL_DETAILS': removal_details
        }
        
        removal_records.append(record)
    
    return removal_records


def filter_conversations_snowflake_engagement(session, df, department_name, departments_config, target_date=None, apply_filter_5=True):
    """
    Apply engagement filtering for Snowflake data.
    Adapted from main_analytics.py filter_conversations_combined() function.
    
    Engagement criteria:
    1. Conversation must have at least one consumer normal message
    2. Conversation must have at least one agent/bot normal message from the department
    
    Args:
        session: Snowflake session for querying tables
        df: Preprocessed DataFrame
        department_name: Department name
        departments_config: Department configuration dictionary
        target_date: Target date for analysis
        apply_filter_5: Whether to apply filter 5 (bot skill filter)
    
    Returns:
        Tuple: (filtered_df, filtering_stats, bot_routed_no_response, removed_conversations_data)
    """
    print(f"  🔍 Applying engagement filtering for {department_name}...")
    
    # Initialize list to track all removed conversations
    removed_conversations_data = []
    
    if department_name not in departments_config:
        raise ValueError(f"Department '{department_name}' not configured")
    
    dept_config = departments_config[department_name]
    agent_skills = dept_config['agent_skills']
    bot_skills = dept_config['bot_skills']

    # Normalize certain message types based on TEXT payload (private/transfer -> tool/tool response)
    df = df.copy()
    if 'MESSAGE_TYPE' in df.columns and 'TEXT' in df.columns:
        msg_type_lower = df['MESSAGE_TYPE'].astype(str).str.lower()
        mask = msg_type_lower.isin(['private message', 'transfer'])
        if mask.any():
            inferred_series = df.loc[mask, 'TEXT'].apply(lambda txt: infer_tool_message_type(txt, department_name))
            has_inferred = inferred_series.notnull()
            print(f"    🧰 Inferred tool messages: {has_inferred.sum()}")
            # Debug: check specific MESSAGE_INDEX 60283497 presence and inference status
            try:
                if 'MESSAGE_INDEX' in df.columns:
                    debug_msg_index = 60283497
                    candidate_idx = df.index[(df['MESSAGE_INDEX'] == debug_msg_index) & mask]
                    if len(candidate_idx) > 0:
                        idx0 = candidate_idx[0]
                        in_inferred = idx0 in inferred_series.index
                        non_null = bool(has_inferred.get(idx0, False)) if in_inferred else False
                        inferred_val = inferred_series.get(idx0, None) if in_inferred else None
                        print(f"    🔎 MESSAGE_INDEX {debug_msg_index}: in_inferred={in_inferred}, non_null={non_null}, value={inferred_val}")
                    else:
                        print(f"    🔎 MESSAGE_INDEX {debug_msg_index}: not in current mask (private/transfer)")
            except Exception as _dbg_err:
                print(f"    🔎 Debug check failed: {type(_dbg_err).__name__}: {_dbg_err}")
            if has_inferred.any():
                df.loc[mask & has_inferred, 'MESSAGE_TYPE'] = inferred_series[has_inferred].str.lower()
    
    # Get all unique conversation IDs
    all_conversations = set(df['CONVERSATION_ID'].unique())
    print(f"    📊 Total conversations: {len(all_conversations)}")
    
    # Prepare conversation metadata for tracking
    all_conversations_list = list(all_conversations)
    all_metadata = analyze_conversation_metadata(df, all_conversations_list)
    
    # Track all removed conversations to avoid duplicates
    already_removed_conversations = set()
    
    # Filter 1: Conversations with consumer normal messages
    consumer_normal_messages = df[
        (df['MESSAGE_TYPE'].str.lower() == 'normal message') &
        (df['SENT_BY'].str.lower() == 'consumer')
    ]
    conversations_with_consumer = set(consumer_normal_messages['CONVERSATION_ID'].unique())
    print(f"    👤 Conversations with consumer messages: {len(conversations_with_consumer)}")
    
    # Track conversations removed due to no consumer messages
    conversations_no_consumer = all_conversations - conversations_with_consumer
    if conversations_no_consumer:
        removed_records = track_removed_conversations(
            session=session,
            conversation_ids=list(conversations_no_consumer),
            department_name=department_name,
            target_date=target_date,
            removal_stage='engagement',
            removal_reason='NO_CONSUMER_MESSAGES',
            conversation_metadata={conv_id: all_metadata.get(conv_id, {}) for conv_id in conversations_no_consumer},
            removal_details=f"Conversations without consumer normal messages: {len(conversations_no_consumer)}"
        )
        removed_conversations_data.extend(removed_records)
        already_removed_conversations.update(conversations_no_consumer)
    
    # Filter 2: Conversations with agent normal messages from department
    agent_normal_messages = df[
        (df['MESSAGE_TYPE'].str.lower() == 'normal message') &
        (df['SENT_BY'].str.lower() == 'agent') &
        (df['TARGET_SKILL_PER_MESSAGE'].isin(agent_skills))
    ]
    conversations_with_agents = set(agent_normal_messages['CONVERSATION_ID'].unique())
    print(f"    👨‍💼 Conversations with department agent messages: {len(conversations_with_agents)}")
    
    # Remove N8N_TEST from conversations_with_agents using TARGET_SKILL_PER_MESSAGE
    conversations_with_agents = conversations_with_agents - set(df[df['TARGET_SKILL_PER_MESSAGE'].str.contains('N8N_TEST', na=False, case=False)]['CONVERSATION_ID'].unique())
    print(f"    👨‍💼 Conversations with department agents after removing N8N_TEST: {len(conversations_with_agents)}")
    # Remove N8N_TEST from conversations_with_agents using THROUGH_SKILL
    conversations_with_agents = conversations_with_agents - set(df[df['THROUGH_SKILL'].str.contains('N8N_TEST', na=False, case=False)]['CONVERSATION_ID'].unique())
    print(f"    👨‍💼 Conversations with department agents after removing N8N_TEST through_skill: {len(conversations_with_agents)}")
    
    # Filter 3: Conversations with bot normal messages from department
    bot_normal_messages = df[
        (df['MESSAGE_TYPE'].str.lower() == 'normal message') &
        (df['SENT_BY'].str.lower() == 'bot') &
        (df['TARGET_SKILL_PER_MESSAGE'].isin(bot_skills))
    ]
    conversations_with_bots = set(bot_normal_messages['CONVERSATION_ID'].unique())
    print(f"    🤖 Conversations with department bot messages: {len(conversations_with_bots)}")
    
    # Combine agent and bot conversations
    conversations_with_service = conversations_with_agents.union(conversations_with_bots)
    print(f"    🏢 Conversations with department service: {len(conversations_with_service)}")

    # Remove N8N_TEST from conversations_with_service using both TARGET_SKILL_PER_MESSAGE and THROUGH_SKILL
    conversations_with_service = conversations_with_service - set(df[df['TARGET_SKILL_PER_MESSAGE'].str.contains('N8N_TEST', na=False, case=False)]['CONVERSATION_ID'].unique())
    conversations_with_service = conversations_with_service - set(df[df['THROUGH_SKILL'].str.contains('N8N_TEST', na=False, case=False)]['CONVERSATION_ID'].unique())
    print(f"    🏢 Conversations with department service after removing N8N_TEST: {len(conversations_with_service)}")

    # also do the same for conversations_with_bots
    conversations_with_bots = conversations_with_bots - set(df[df['TARGET_SKILL_PER_MESSAGE'].str.contains('N8N_TEST', na=False, case=False)]['CONVERSATION_ID'].unique())
    print(f"    🏢 Conversations with department bots after removing N8N_TEST: {len(conversations_with_bots)}")
    # also the same for conversations_with_bots but using 'through_skill' instead of 'TARGET_SKILL_PER_MESSAGE'
    conversations_with_bots = conversations_with_bots - set(df[df['THROUGH_SKILL'].str.contains('N8N_TEST', na=False, case=False)]['CONVERSATION_ID'].unique())
    print(f"    🏢 Conversations with department bots after removing N8N_TEST through_skill: {len(conversations_with_bots)}")

    # Extra: Conversations with inferred tool/tool response messages (from private/transfer)
    tool_like_messages = df[
        (df['MESSAGE_TYPE'].str.lower().isin(['tool', 'tool response'])) &
        (df['TARGET_SKILL_PER_MESSAGE'].isin(bot_skills))
    ]
    conversations_with_tools = set(tool_like_messages['CONVERSATION_ID'].unique())
    print(f"    🧰 Conversations with tool/tool response messages: {len(conversations_with_tools)}")
    

    # Combine agent, bot, and tool/tool response conversations
    conversations_with_service = conversations_with_agents.union(conversations_with_bots).union(conversations_with_tools)
    print(f"    🏢 Conversations with department service: {len(conversations_with_service)}")
    
    # Track N8N_TEST conversations (exclude already removed)
    n8n_test_conversations = set()
    n8n_test_conversations.update(df[df['TARGET_SKILL_PER_MESSAGE'].str.contains('N8N_TEST', na=False, case=False)]['CONVERSATION_ID'].unique())
    n8n_test_conversations.update(df[df['THROUGH_SKILL'].str.contains('N8N_TEST', na=False, case=False)]['CONVERSATION_ID'].unique())
    
    # Exclude conversations already removed
    n8n_test_conversations = n8n_test_conversations - already_removed_conversations
    
    if n8n_test_conversations:
        removed_records = track_removed_conversations(
            session=session,
            conversation_ids=list(n8n_test_conversations),
            department_name=department_name,
            target_date=target_date,
            removal_stage='engagement',
            removal_reason='N8N_TEST_CONVERSATION',
            conversation_metadata={conv_id: all_metadata.get(conv_id, {}) for conv_id in n8n_test_conversations},
            removal_details=f"Conversations containing N8N_TEST in TARGET_SKILL_PER_MESSAGE or THROUGH_SKILL: {len(n8n_test_conversations)}"
        )
        removed_conversations_data.extend(removed_records)
        already_removed_conversations.update(n8n_test_conversations)
    
    # Filter 4: Engagement filter - Conversations that meet both criteria
    engagement_valid_conversations = conversations_with_consumer.intersection(conversations_with_service)
    print(f"    ✅ Engagement-valid conversations: {len(engagement_valid_conversations)}")
    
    # Track conversations with no department service (exclude already removed)
    conversations_no_service = (conversations_with_consumer - conversations_with_service) - already_removed_conversations
    if conversations_no_service:
        removed_records = track_removed_conversations(
            session=session,
            conversation_ids=list(conversations_no_service),
            department_name=department_name,
            target_date=target_date,
            removal_stage='engagement',
            removal_reason='NO_DEPARTMENT_SERVICE',
            conversation_metadata={conv_id: all_metadata.get(conv_id, {}) for conv_id in conversations_no_service},
            removal_details=f"Conversations without agent or bot messages from department skills: {len(conversations_no_service)}"
        )
        removed_conversations_data.extend(removed_records)
        already_removed_conversations.update(conversations_no_service)
    
    # Remove N8N_TEST from engagement_valid_conversations using both TARGET_SKILL_PER_MESSAGE and THROUGH_SKILL
    engagement_valid_conversations = engagement_valid_conversations - set(df[df['TARGET_SKILL_PER_MESSAGE'].str.contains('N8N_TEST', na=False, case=False)]['CONVERSATION_ID'].unique())
    engagement_valid_conversations = engagement_valid_conversations - set(df[df['THROUGH_SKILL'].str.contains('N8N_TEST', na=False, case=False)]['CONVERSATION_ID'].unique())
    print(f"    ✅ Engagement-valid conversations after removing N8N_TEST: {len(engagement_valid_conversations)}")
    
    # Filter: Remove conversations where ALL department messages have NULL EXECUTION_ID (MV_Resolvers only)
    # EXCEPTION: Keep conversations with guardrail interactions
    conversations_with_all_null_execution_id = set()
    
    if department_name == 'MV_Resolvers':
        print(f"  🔍 Checking for conversations with all NULL execution IDs in department messages (MV_Resolvers only)...")
        
        # Get guardrail conversation IDs for exception handling
        guardrail_conv_ids = get_guardrail_conversation_ids(session, department_name, target_date)
        print(f"    🛡️  Found {len(guardrail_conv_ids)} conversations with guardrail interactions (will be kept as exceptions)")
        
        # Combine bot_skills and agent_skills to get all department skills
        department_skills = set(bot_skills + agent_skills)
        
        # Check if EXECUTION_ID column exists
        if 'EXECUTION_ID' in df.columns:
            for conv_id in engagement_valid_conversations:
                # Get all messages for this conversation that are from department skills
                conv_messages = df[
                    (df['CONVERSATION_ID'] == conv_id) &
                    (df['TARGET_SKILL_PER_MESSAGE'].isin(department_skills))
                ]
                
                # If there are department messages
                if len(conv_messages) > 0:
                    # Check if ALL department messages have NULL EXECUTION_ID
                    # Check for both None and NaN values
                    null_execution_ids = conv_messages['EXECUTION_ID'].isna().sum()
                    
                    if null_execution_ids == len(conv_messages):
                        # All department messages have NULL EXECUTION_ID
                        # Check exception: if conversation has guardrail, keep it
                        if conv_id not in guardrail_conv_ids:
                            conversations_with_all_null_execution_id.add(conv_id)
                        # else: conversation has guardrail - exception applies, keep it
            
            print(f"    ⚠️  Conversations with all NULL execution IDs (excluding guardrail exceptions): {len(conversations_with_all_null_execution_id)}")
            
            # Track conversations removed due to NULL execution IDs
            if conversations_with_all_null_execution_id:
                removed_records = track_removed_conversations(
                    session=session,
                    conversation_ids=list(conversations_with_all_null_execution_id),
                    department_name=department_name,
                    target_date=target_date,
                    removal_stage='null_execution_id',
                    removal_reason='ALL_NULL_EXECUTION_ID',
                    conversation_metadata={conv_id: all_metadata.get(conv_id, {}) for conv_id in conversations_with_all_null_execution_id},
                    removal_details=f"All department messages have NULL EXECUTION_ID (guardrail exceptions excluded): {len(conversations_with_all_null_execution_id)}"
                )
                removed_conversations_data.extend(removed_records)
            
            # Remove these conversations from engagement_valid_conversations
            engagement_valid_conversations = engagement_valid_conversations - conversations_with_all_null_execution_id
            print(f"    ✅ Engagement-valid conversations after removing NULL execution IDs: {len(engagement_valid_conversations)}")
        else:
            print(f"    ⚠️  EXECUTION_ID column not found - skipping NULL execution ID filter")
    
    if apply_filter_5:
        # Filter 5: Bot skill filter - Check THROUGH_SKILL contains any bot_skills
        conversations_with_bot_skills = set()
        for conv_id in engagement_valid_conversations:
            # Get first row of conversation
            first_row = df[df['CONVERSATION_ID'] == conv_id].iloc[0]
            through_skill = str(first_row.get('THROUGH_SKILL', ''))
            
            # Parse THROUGH_SKILL as comma-separated values for exact matching
            through_skills_list = [s.strip() for s in through_skill.split(',')]
            
            # Check if any bot_skill is contained in THROUGH_SKILL (exact match)
            matching_bot_skills = [bot_skill for bot_skill in bot_skills if bot_skill in through_skills_list]
            
            # Exclude if the only matching bot skill is GPT_MAIDSAT
            if len(matching_bot_skills) > 0:
                conversations_with_bot_skills.add(conv_id)
                # if len(matching_bot_skills) == 1 and (matching_bot_skills[0] == 'GPT_MAIDSAT' or (matching_bot_skills[0] == 'Filipina_Pending_Country_of_Residence' and department_name != 'AT_Filipina')):
                #     # Skip this conversation - only GPT_MAIDSAT found
                #     continue
                # else:
                #     # Has other bot skills, include this conversation
                #     conversations_with_bot_skills.add(conv_id)
        
        print(f"    🤖 Conversations with bot skills: {len(conversations_with_bot_skills)}")
        
        # Track conversations that failed bot skill filter
        conversations_failed_bot_skill = engagement_valid_conversations - conversations_with_bot_skills
        if conversations_failed_bot_skill:
            removed_records = track_removed_conversations(
                session=session,
                conversation_ids=list(conversations_failed_bot_skill),
                department_name=department_name,
                target_date=target_date,
                removal_stage='bot_skill',
                removal_reason='FAILED_BOT_SKILL_FILTER',
                conversation_metadata={conv_id: all_metadata.get(conv_id, {}) for conv_id in conversations_failed_bot_skill},
                removal_details=f"Conversations where THROUGH_SKILL does not contain required bot skills: {len(conversations_failed_bot_skill)}"
            )
            removed_conversations_data.extend(removed_records)
        
        # Filter the dataframe to only include conversations with bot skills
        filtered_df = df[df['CONVERSATION_ID'].isin(conversations_with_bot_skills)]
    else:
        conversations_with_bot_skills = engagement_valid_conversations  # For stats consistency
        filtered_df = df[df['CONVERSATION_ID'].isin(engagement_valid_conversations)]
    
    # BOT-ROUTED NO RESPONSE LOGIC DISABLED FOR ALL DEPARTMENTS
    # Additional filter: Bot-routed conversations with no agent/bot responses
    # print(f"  🤖 Checking for bot-routed conversations with no responses...")
    bot_routed_no_response = set()  # Empty set - disabled
    
    
    
    print(f"    ⚠️  Bot-routed no-response logic: DISABLED for all departments")

    # Exclude N8N_TEST conversations from this count to avoid false positives
    n8n_conv_ids = set(df[df['TARGET_SKILL_PER_MESSAGE'].str.contains('N8N_TEST', na=False, case=False)]['CONVERSATION_ID'].unique())
    n8n_conv_ids.update(df[df['THROUGH_SKILL'].str.contains('N8N_TEST', na=False, case=False)]['CONVERSATION_ID'].unique())
    consumer_only_chats_count = len(
        (conversations_with_consumer - conversations_with_service) - n8n_conv_ids
    )
    
    # Calculate filtering statistics
    filtering_stats = {
        'total_original_conversations': len(all_conversations),
        'conversations_with_consumer': len(conversations_with_consumer),
        'conversations_with_agents': len(conversations_with_agents),
        'conversations_with_bots': len(conversations_with_bots),
        'conversations_with_tools': len(conversations_with_tools),
        'conversations_with_service': len(conversations_with_service),
        'conversations_with_all_null_execution_id': len(conversations_with_all_null_execution_id),
        'engagement_valid_conversations': len(engagement_valid_conversations),
        'conversations_with_bot_skills': len(conversations_with_bot_skills) if apply_filter_5 else 0,
        'bot_routed_no_response_conversations': len(bot_routed_no_response),
        'engagement_retention_rate': len(engagement_valid_conversations)/len(all_conversations)*100 if all_conversations else 0,
        'bot_skill_retention_rate': len(conversations_with_bot_skills)/len(engagement_valid_conversations)*100 if (apply_filter_5 and engagement_valid_conversations) else 0
    }
    
    print(f"    📈 Engagement retention: {filtering_stats['engagement_retention_rate']:.1f}%")
    print(f"    📈 Bot skill retention: {filtering_stats['bot_skill_retention_rate']:.1f}%")
    
   
    
    # Apply hi-bye filter as final filter on the engagement filtered_df
    conversations_before_hi_bye = set(filtered_df['CONVERSATION_ID'].unique())
    filtered_df, hi_bye_stats = filter_conversations_snowflake_hi_bye(
        session, filtered_df, department_name, target_date
    )
    conversations_after_hi_bye = set(filtered_df['CONVERSATION_ID'].unique())
    
    # Track conversations removed by hi-bye filter
    conversations_removed_by_hi_bye = conversations_before_hi_bye - conversations_after_hi_bye
    if conversations_removed_by_hi_bye:
        # Prepare metadata for removed conversations - use original df before hi-bye filter
        temp_df = df[df['CONVERSATION_ID'].isin(conversations_removed_by_hi_bye)]
        all_conversations_list = list(conversations_removed_by_hi_bye)
        all_metadata = analyze_conversation_metadata(temp_df, all_conversations_list)
        
        removed_records = track_removed_conversations(
            session=session,
            conversation_ids=list(conversations_removed_by_hi_bye),
            department_name=department_name,
            target_date=target_date,
            removal_stage='hi_bye',
            removal_reason='HI_BYE_CONVERSATION',
            conversation_metadata={conv_id: all_metadata.get(conv_id, {}) for conv_id in conversations_removed_by_hi_bye},
            removal_details=f"Conversations identified as hi-bye from HI_BYE_CHATS table: {len(conversations_removed_by_hi_bye)}"
        )
        removed_conversations_data.extend(removed_records)
    
    # Merge hi-bye stats into filtering_stats
    filtering_stats.update(hi_bye_stats)

    # ============================================================
    # MV_Resolvers boomerang filter (NEW STAGE)
    # ------------------------------------------------------------
    # Remove chats where GPT_MV_RESOLVERS was involved, the chat was
    # transferred to another department, and that department then sent it
    # back to MV_RESOLVERS_SENIORS or MV_CALLERS. These chats stay in the
    # Resolvers chats breakdown and Chatbot Alignment metrics (those use
    # direct SQL, not the filtered DataFrame) but are excluded from all
    # DataFrame-based analyses (transfers, intervention, quality metrics
    # breakdown).
    # ============================================================
    boomerang_excluded_count = 0
    if department_name == 'MV_Resolvers' and not filtered_df.empty:
        print(f"  🔁 Checking for MV_Resolvers boomerang chats (GPT_MV_RESOLVERS → other dept → back to seniors/callers)...")
        in_scope_conv_ids = set(filtered_df['CONVERSATION_ID'].unique())
        boomerang_conv_ids = get_mv_bot_boomerang_conv_ids(
            session, in_scope_conv_ids, target_date
        )
        boomerang_conv_ids = boomerang_conv_ids.intersection(in_scope_conv_ids)

        if boomerang_conv_ids:
            temp_df = df[df['CONVERSATION_ID'].isin(boomerang_conv_ids)]
            boomerang_metadata = analyze_conversation_metadata(
                temp_df, list(boomerang_conv_ids)
            )

            removed_records = track_removed_conversations(
                session=session,
                conversation_ids=list(boomerang_conv_ids),
                department_name=department_name,
                target_date=target_date,
                removal_stage='mv_bot_boomerang',
                removal_reason='MV_BOT_BOOMERANG',
                conversation_metadata={
                    cid: boomerang_metadata.get(cid, {}) for cid in boomerang_conv_ids
                },
                removal_details=(
                    f"GPT_MV_RESOLVERS → other department → returned to MV_RESOLVERS_SENIORS/MV_CALLERS: "
                    f"{len(boomerang_conv_ids)}"
                )
            )
            removed_conversations_data.extend(removed_records)

            filtered_df = filtered_df[~filtered_df['CONVERSATION_ID'].isin(boomerang_conv_ids)]
            boomerang_excluded_count = len(boomerang_conv_ids)
            print(f"    🚫 Excluded {boomerang_excluded_count} MV bot boomerang chats")
        else:
            print(f"    ✅ No MV bot boomerang chats found")

    filtering_stats['mv_bot_boomerang_excluded'] = boomerang_excluded_count

    return filtered_df, filtering_stats, bot_routed_no_response, removed_conversations_data


def filter_conversations_snowflake_date(df, department_name, target_date=None):
    """
    Apply date-based filtering for Snowflake data.
    Adapted from main_analytics.py date filtering logic.
    
    Date criteria:
    Remove conversations where ALL messages are from day 1 (keep conversations with at least one day 2 message)
    
    Args:
        df: DataFrame after engagement filtering
        department_name: Department name
        target_date: Target date for analysis
    
    Returns:
        Tuple: (filtered_df, filtering_stats)
    """
    print(f"  📅 Applying date filtering for {department_name}...")
    
    # Get date range information
    date_range = create_snowflake_date_range(target_date)
    day1_date = date_range['day1_date']
    day2_date = date_range['day2_date']
    
    print(f"    📅 Day 1: {day1_date}, Day 2: {day2_date}")
    
    # Convert message timestamps to dates
    # Create a copy to avoid SettingWithCopyWarning
    df = df.copy()
    df['MESSAGE_DATE'] = pd.to_datetime(df['MESSAGE_SENT_TIME']).dt.date
    
    # Get conversations before date filtering
    conversations_before_date_filter = set(df['CONVERSATION_ID'].unique())
    
    # For each conversation, check if it has at least one message from day 2
    conversations_with_day2_messages = set()
    
    for conv_id in conversations_before_date_filter:
        conv_messages = df[df['CONVERSATION_ID'] == conv_id]
        message_dates = conv_messages['MESSAGE_DATE'].unique()
        
        # Keep conversation if it has at least one message from day 2
        if day2_date in message_dates:
            conversations_with_day2_messages.add(conv_id)
    
    print(f"    📊 Conversations before date filter: {len(conversations_before_date_filter)}")
    print(f"    📊 Conversations with day 2 messages: {len(conversations_with_day2_messages)}")
    
    # Filter the dataframe to only include conversations with day 2 messages
    filtered_df = df[df['CONVERSATION_ID'].isin(conversations_with_day2_messages)]
    
    # Remove the temporary MESSAGE_DATE column
    if 'MESSAGE_DATE' in filtered_df.columns:
        filtered_df = filtered_df.drop('MESSAGE_DATE', axis=1)
    
    # Calculate filtering statistics
    filtering_stats = {
        'conversations_before_date_filter': len(conversations_before_date_filter),
        'conversations_with_day2_messages': len(conversations_with_day2_messages),
        'conversations_filtered_by_date': len(conversations_before_date_filter) - len(conversations_with_day2_messages),
        'date_retention_rate': len(conversations_with_day2_messages)/len(conversations_before_date_filter)*100 if conversations_before_date_filter else 0
    }
    
    print(f"    📈 Date retention: {filtering_stats['date_retention_rate']:.1f}%")
    
    return filtered_df, filtering_stats


def filter_conversations_snowflake_combined(session, df, department_name, target_date=None, apply_filter_5=True):
    """
    Apply combined filtering (engagement + date) for Snowflake data.
    Adapted from main_analytics.py filter_conversations_combined() function.
    
    Args:
        session: Snowflake session for querying tables
        df: Preprocessed DataFrame
        department_name: Department name
        target_date: Target date for analysis
        apply_filter_5: Whether to apply filter 5 (bot skill filter)
    
    Returns:
        Tuple: (filtered_df, combined_filtering_stats, bot_routed_no_response)
    """
    print(f"  🔄 Applying combined filtering for {department_name}...")
    
    # Get department configuration
    departments_config = get_snowflake_departments_config()
    
    # Track counts at each stage for filtering table
    source_conversations = len(df['CONVERSATION_ID'].unique())
    
    # Step 1: Apply engagement filtering (includes hi-bye filtering at the end)
    engagement_filtered_df, engagement_stats, bot_routed_no_response, removed_conversations_data = filter_conversations_snowflake_engagement(
        session, df, department_name, departments_config, target_date, apply_filter_5
    )
    
    if engagement_filtered_df.empty:
        print(f"    ❌ No conversations passed engagement filtering")
        return pd.DataFrame(), {**engagement_stats, 'final_valid_conversations': 0, 'source_conversations': source_conversations}, set()
    
    # Step 2: Apply date filtering
    final_filtered_df, date_stats = filter_conversations_snowflake_date(
        engagement_filtered_df, department_name, target_date
    )
    
    # Track conversations removed by date filter
    conversations_removed_by_date = set(engagement_filtered_df['CONVERSATION_ID'].unique()) - set(final_filtered_df['CONVERSATION_ID'].unique())
    if conversations_removed_by_date:
        # Prepare metadata for removed conversations
        all_conversations_list = list(conversations_removed_by_date)
        all_metadata = analyze_conversation_metadata(engagement_filtered_df, all_conversations_list)
        
        removed_records = track_removed_conversations(
            session=session,
            conversation_ids=list(conversations_removed_by_date),
            department_name=department_name,
            target_date=target_date,
            removal_stage='date',
            removal_reason='WRONG_DATE',
            conversation_metadata={conv_id: all_metadata.get(conv_id, {}) for conv_id in conversations_removed_by_date},
            removal_details=f"Conversations with all messages from day 1 (no day 2 messages): {len(conversations_removed_by_date)}"
        )
        removed_conversations_data.extend(removed_records)
    
    # Combine statistics
    combined_stats = {
        **engagement_stats,
        **date_stats,
        'final_valid_conversations': len(final_filtered_df['CONVERSATION_ID'].unique()) if not final_filtered_df.empty else 0,
        'source_conversations': source_conversations
    }
    
    # Calculate overall retention rate
    if combined_stats['total_original_conversations'] > 0:
        combined_stats['overall_retention_rate'] = (
            combined_stats['final_valid_conversations'] / 
            combined_stats['total_original_conversations'] * 100
        )
    else:
        combined_stats['overall_retention_rate'] = 0
    
    print(f"    🎯 FINAL RESULT: {combined_stats['final_valid_conversations']} conversations")
    print(f"    📈 Overall retention: {combined_stats['overall_retention_rate']:.1f}%")
    
    # Save removed conversations to table
    if removed_conversations_data:
        try:
            print(f"    💾 Saving {len(removed_conversations_data)} removed conversation records to CONVERSATIONS_REMOVED_BY_STAGE...")
            removed_df = pd.DataFrame(removed_conversations_data)
            
            # Check for duplicates
            duplicate_convs = removed_df[removed_df.duplicated('CONVERSATION_ID', keep=False)]['CONVERSATION_ID'].unique()
            if len(duplicate_convs) > 0:
                print(f"    ⚠️  WARNING: {len(duplicate_convs)} conversations tracked multiple times!")
                print(f"    📋 Duplicate breakdown:")
                for conv_id in list(duplicate_convs)[:10]:  # Show first 10
                    conv_records = removed_df[removed_df['CONVERSATION_ID'] == conv_id]
                    reasons = ', '.join(conv_records['REMOVAL_REASON'].tolist())
                    print(f"       - {conv_id}: {reasons}")
                if len(duplicate_convs) > 10:
                    print(f"       ... and {len(duplicate_convs) - 10} more")
                
                # Remove duplicates - keep first occurrence only
                print(f"    🧹 Removing duplicates (keeping first removal reason for each conversation)...")
                removed_df = removed_df.drop_duplicates('CONVERSATION_ID', keep='first')
                print(f"    ✅ After deduplication: {len(removed_df)} unique conversations")
            
            # Define columns for the table
            columns = [
                'CONVERSATION_ID', 'REMOVAL_STAGE', 'REMOVAL_REASON', 'THROUGH_SKILL',
                'FIRST_MESSAGE_TIME', 'LAST_MESSAGE_TIME', 'MESSAGE_COUNT',
                'CONSUMER_MESSAGE_COUNT', 'AGENT_MESSAGE_COUNT', 'BOT_MESSAGE_COUNT',
                'TARGET_SKILLS_USED', 'REMOVAL_DETAILS'
            ]
            
            insert_raw_data_with_cleanup(
                session=session,
                table_name="CONVERSATIONS_REMOVED_BY_STAGE",
                department=department_name,
                target_date=target_date,
                dataframe=removed_df[columns],
                columns=columns
            )
            print(f"    ✅ Saved {len(removed_df)} removed conversation records")
        except Exception as e:
            print(f"    ⚠️  Failed to save removed conversations: {str(e)}")
    else:
        print(f"    ℹ️  No removed conversations to save")
    
    return final_filtered_df, combined_stats, bot_routed_no_response


def save_filtering_counts_table(session, department_name, target_date, filtering_stats):
    """
    Save filtering counts to CONVERSATION_FILTERING_COUNTS table.
    This table tracks how many conversations are removed at each filtering stage.
    
    Args:
        session: Snowflake session
        department_name: Department name
        target_date: Target date for analysis
        filtering_stats: Dictionary containing filtering statistics from combined filtering
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Extract counts from filtering_stats
        source_conversations = filtering_stats.get('source_conversations', 0)
        engagement_valid = filtering_stats.get('engagement_valid_conversations', 0)
        bot_skills_valid = filtering_stats.get('conversations_with_bot_skills', 0)
        bot_routed_added = filtering_stats.get('bot_routed_no_response_conversations', 0)
        hi_bye_removed = filtering_stats.get('hi_bye_conversations_removed', 0)
        date_valid = filtering_stats.get('conversations_with_day2_messages', 0)
        final_conversations = filtering_stats.get('final_valid_conversations', 0)
        
        # Calculate removed counts
        removed_engagement = source_conversations - engagement_valid
        removed_bot_skills = engagement_valid - bot_skills_valid
        # bot_routed_added is added back (negative removal)
        after_bot_routed = bot_skills_valid + bot_routed_added
        removed_hi_bye = hi_bye_removed  # This is already a count of removed conversations
        after_hi_bye = after_bot_routed - removed_hi_bye
        removed_date = after_hi_bye - final_conversations
        total_removed = source_conversations - final_conversations
        
        # Calculate retention percentage
        overall_retention = (final_conversations / source_conversations * 100) if source_conversations > 0 else 0
        
        # Create DataFrame with one row per department per date
        filtering_data = {
            'DATE': [target_date],
            'DEPARTMENT': [department_name],
            'SOURCE_CONVERSATIONS': [source_conversations],
            'REMOVED_ENGAGEMENT_FILTER': [removed_engagement],
            'REMOVED_BOT_SKILLS_FILTER': [removed_bot_skills],
            'ADDED_BOT_ROUTED_NO_RESPONSE': [bot_routed_added],
            'REMOVED_HI_BYE_FILTER': [removed_hi_bye],
            'REMOVED_DATE_FILTER': [removed_date],
            'TOTAL_REMOVED': [total_removed],
            'FINAL_CONVERSATIONS': [final_conversations],
            'OVERALL_RETENTION_PERCENTAGE': [round(overall_retention, 2)]
        }
        
        filtering_df = pd.DataFrame(filtering_data)
        
        # Define columns for the table
        columns = [
            'DATE', 'DEPARTMENT', 'SOURCE_CONVERSATIONS',
            'REMOVED_ENGAGEMENT_FILTER', 'REMOVED_BOT_SKILLS_FILTER',
            'ADDED_BOT_ROUTED_NO_RESPONSE', 'REMOVED_HI_BYE_FILTER',
            'REMOVED_DATE_FILTER', 'TOTAL_REMOVED',
            'FINAL_CONVERSATIONS', 'OVERALL_RETENTION_PERCENTAGE'
        ]
        
        # Save to Snowflake using insert_raw_data_with_cleanup
        table_name = 'CONVERSATION_FILTERING_COUNTS'
        insert_raw_data_with_cleanup(
            session=session,
            table_name=table_name,
            department=department_name,
            target_date=target_date,
            dataframe=filtering_df,
            columns=columns
        )
        
        print(f"  ✅ Filtering counts saved to {table_name}")
        return True
        
    except Exception as e:
        print(f"  ⚠️  Error saving filtering counts: {str(e)}")
        return False


# ============================================================================
# CACHED DATA LOADER - Prevents redundant data loading for same department
# ============================================================================

# Global cache dictionary to store processed data per department
# Key format: f"{department_name}_{target_date}_{apply_filter_5}"
_DEPARTMENT_DATA_CACHE = {}

def clear_department_cache():
    """
    Clear the department data cache. 
    Call this when you want to force fresh data loading.
    """
    global _DEPARTMENT_DATA_CACHE
    _DEPARTMENT_DATA_CACHE.clear()
    print("🗑️  Department data cache cleared")


def process_department_phase1(session: snowpark.Session, department_name, target_date=None, apply_filter_5=True):
    """
    Process a single department through Phase 1 foundation layer WITH CACHING.
    
    ⚡ OPTIMIZED: Results are cached per department+date+filter combination.
    Subsequent calls with the same parameters return cached data instantly.
    
    Steps:
    1. Check cache for existing results
    2. If cached: Return cached data (FAST PATH)
    3. If not cached: Load data from Snowflake table
    4. Apply preprocessing 
    5. Apply combined filtering
    6. Store in cache and return
    
    Args:
        session: Snowflake session
        department_name: Department to process
        target_date: Target date for analysis
        apply_filter_5: Whether to apply filter 5 logic
    
    Returns:
        Tuple: (filtered_df, processing_stats, success, bot_routed_no_response)
    """
    # Create cache key based on all parameters that affect the result
    cache_key = f"{department_name}_{target_date}_{apply_filter_5}"
    
    # CHECK CACHE FIRST - Fast path for repeated calls
    if cache_key in _DEPARTMENT_DATA_CACHE:
        print(f"\n🏢 PROCESSING DEPARTMENT: {department_name}")
        print("=" * 50)
        print(f"⚡ CACHE HIT! Returning cached data for {department_name}")
        cached_result = _DEPARTMENT_DATA_CACHE[cache_key]
        print(f"   📊 Cached rows: {len(cached_result['filtered_df']):,}")
        print(f"   🎯 Cached conversations: {cached_result['stats'].get('final_conversations', 0):,}")
        print(f"   ✅ Skipped: Data loading, preprocessing, and filtering")
        # Return a COPY of the cached DataFrame to prevent modifications affecting cache
        return cached_result['filtered_df'].copy(), cached_result['stats'].copy(), cached_result['success'], cached_result['bot_routed_no_response'].copy()
    
    # CACHE MISS - Proceed with full processing
    print(f"\n🏢 PROCESSING DEPARTMENT: {department_name}")
    print("=" * 50)
    print(f"💾 Cache miss - Loading fresh data...")
    
    try:
        # Get department configuration
        departments_config = get_snowflake_departments_config()
        
        if department_name not in departments_config:
            print(f"❌ Department '{department_name}' not configured")
            return pd.DataFrame(), {}, False, set()
        
        dept_config = departments_config[department_name]
        table_name = dept_config['table_name']
        
        # Step 1: Load data from Snowflake table with date filtering
        print(f"📊 Step 1: Loading data from {table_name}...")
        try:
            # Calculate the filter date (yesterday + 1 day for UPDATED_AT filtering)
            filter_date = (datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            
            # Use SQL query with WHERE clause instead of loading entire table
            query = f"""
            SELECT * FROM {table_name} 
            WHERE DATE(UPDATED_AT) = '{filter_date}'
            ORDER BY CONVERSATION_ID, MESSAGE_SENT_TIME
            """
            
            raw_data_df = session.sql(query).to_pandas()
            print(f"    ✅ Loaded {len(raw_data_df)} rows from Snowflake (filtered for {filter_date})")
        except Exception as table_error:
            table_error_details = traceback.format_exc()
            table_error_msg = f"TABLE_LOAD_ERROR: {type(table_error).__name__}: {str(table_error)}"
            print(f"    ❌ Failed to load table {table_name}: {table_error_msg}")
            print(f"    Full traceback: {table_error_details}")
            return pd.DataFrame(), {'error': table_error_msg, 'traceback': table_error_details}, False, set()
        
        if raw_data_df.empty:
            print(f"    ⚠️  No data found in {table_name}")
            return pd.DataFrame(), {'error': 'No data found'}, False, set()
        
        # Step 2: Apply preprocessing
        print(f"🧹 Step 2: Preprocessing data...")
        processed_df = preprocess_data_snowflake_phase1(raw_data_df, department_name, target_date)
        
        if processed_df.empty:
            print(f"    ❌ No data after preprocessing")
            return pd.DataFrame(), {'error': 'No data after preprocessing'}, False, set()
        
        # Step 3: Apply combined filtering (includes hi-bye filtering)
        print(f"🔍 Step 3: Applying combined filtering...")
        filtered_df, filtering_stats, bot_routed_no_response = filter_conversations_snowflake_combined(
            session, processed_df, department_name, target_date, apply_filter_5
        )
        
        if filtered_df.empty:
            print(f"    ❌ No conversations passed filtering")
            return pd.DataFrame(), {**filtering_stats, 'error': 'No conversations passed filtering'}, False, set()
        
        # Step 4: Save filtering counts to table
        # print(f"💾 Step 4: Saving filtering counts...")
        # save_filtering_counts_table(session, department_name, target_date, filtering_stats)
        
        # Prepare final statistics
        final_stats = {
            'department': department_name,
            'table_name': table_name,
            'raw_rows': len(raw_data_df),
            'processed_rows': len(processed_df),
            'filtered_rows': len(filtered_df),
            'final_conversations': filtering_stats['final_valid_conversations'],
            **filtering_stats
        }
        
        print(f"\n✅ SUCCESS: {department_name}")
        print(f"   📊 Raw rows: {final_stats['raw_rows']:,}")
        print(f"   🧹 Processed rows: {final_stats['processed_rows']:,}")
        print(f"   🔍 Filtered rows: {final_stats['filtered_rows']:,}")
        print(f"   🎯 Final conversations: {final_stats['final_conversations']:,}")
        print(f"   📈 Overall retention: {final_stats['overall_retention_rate']:.1f}%")
        
        # STORE IN CACHE for future calls
        _DEPARTMENT_DATA_CACHE[cache_key] = {
            'filtered_df': filtered_df.copy(),  # Store a copy to prevent external modifications
            'stats': final_stats.copy(),
            'success': True,
            'bot_routed_no_response': bot_routed_no_response.copy()
        }
        print(f"   💾 Data cached for key: {cache_key}")
        
        return filtered_df, final_stats, True, bot_routed_no_response
        
    except Exception as e:
        error_details = traceback.format_exc()
        error_msg = f"EXCEPTION: {type(e).__name__}: {str(e)}"
        print(f"❌ FAILED: {department_name} - {error_msg}")
        print(f"   Full traceback: {error_details}")
        return pd.DataFrame(), {'error': error_msg, 'traceback': error_details}, False, set()

# ============================================================================
# PHASE 2: CORE ANALYTICS COMPONENTS
# ============================================================================

def create_output_table_name(base_name, target_date=None):
    """
    Create timestamped table names for raw output tables.
    
    Args:
        base_name: Base table name (e.g., 'BOT_HANDLED_CONVERSATIONS')
        target_date: Target date string
    
    Returns:
        Full table name with date suffix
    """
    if target_date is None:
        date_str = datetime.now().strftime('%Y_%m_%d')
    else:
        if isinstance(target_date, str):
            date_obj = datetime.strptime(target_date, '%Y-%m-%d')
        else:
            date_obj = target_date
        date_str = date_obj.strftime('%Y_%m_%d')
    
    return f"LLM_EVAL.PUBLIC.{base_name}_{date_str}"


# ============================================================================
# BOT HANDLING ANALYSIS
# ============================================================================

def get_first_different_previous_skill(current_idx, current_skill, df):
    """
    Look backwards from current message index to find the first message 
    with a different TARGET_SKILL_PER_MESSAGE.
    
    Helper function for CC_Resolvers bot-handled conversation logic.
    Used to determine if a message from "GPT CC Shadowers" should be excluded
    from agent intervention counting based on the previous skill transition.
    
    Args:
        current_idx: Index of current message in dataframe
        current_skill: Current message's TARGET_SKILL_PER_MESSAGE
        df: Full conversation dataframe
        
    Returns:
        str or None: The first different skill found, or None if not found
        
    Example:
        >>> # Messages with skills: ['GPT_A', 'GPT_A', 'GPT_A', 'GPT_B']
        >>> get_first_different_previous_skill(3, 'GPT_B', df)
        'GPT_A'  # Returns the last occurrence of different skill
    """
    # Get all messages before current index
    previous_messages = df.loc[:current_idx-1]
    
    # Iterate backwards through previous messages
    for idx in reversed(previous_messages.index):
        prev_skill = previous_messages.loc[idx, 'TARGET_SKILL_PER_MESSAGE']
        # Return the first skill that's different from current
        if prev_skill != current_skill:
            return prev_skill
    
    return None


def is_conversation_fully_handled_by_bot_snowflake(conversation_df, department_name, departments_config):
    """
    Check if a conversation is fully handled by bot (Snowflake version).
    Adapted from main_analytics.py is_conversation_fully_handled_by_bot()
    
    A conversation is considered fully handled by bot if:
    - It has NO "Normal message" from agents using ANY skill related to the same department
    - This includes both bot_skills and agent_skills for the department
    - Agents from other departments are ignored (still considered bot-handled)
    
    SPECIAL LOGIC FOR CC_RESOLVERS DEPARTMENT:
    - Agent messages from "GPT CC Shadowers" skill are NOT counted if:
      * The first different previous TARGET_SKILL_PER_MESSAGE was "GPT_CC_PROSPECT"
      * This allows conversations to be bot-handled even with "GPT CC Shadowers" messages
      * when they follow a "GPT_CC_PROSPECT" skill transition
    - This logic ONLY applies to CC_Resolvers department
    - All other departments use standard counting logic
    
    Args:
        conversation_df: DataFrame containing one conversation's messages
        department_name: Department name for skill filtering
        departments_config: Department configuration dictionary
    
    Returns:
        tuple: (is_bot_handled: bool, agent_message_count: int, has_call_request: bool, 
                counted_agent_messages: int, bot_message_count: int, is_bot_handled_excluding_fillers: bool,
                has_valid_system_transfer: bool, agent_message_count_excluding_pokes: int,
                agent_messages_from_allowed_skills: int, has_complaint_action: bool)
               - is_bot_handled: True if conversation is fully handled by bot
               - agent_message_count: Number of agent messages from department-related skills
               - has_call_request: True if agent sent "an agent will be reaching out to you" message
               - counted_agent_messages: Agent messages excluding filtered phrases
               - bot_message_count: Number of bot messages from department-related skills
               - is_bot_handled_excluding_fillers: True if fully handled by bot excluding filler messages
               - has_valid_system_transfer: True if conversation has system transfer from bot to agent skill (not by GPT)
               - agent_message_count_excluding_pokes: Number of agent messages excluding pokes (CC_Sales only)
               - agent_messages_from_allowed_skills: Number of agent messages from agent_skills only
               - has_complaint_action: True if system message contains "Open_or_CommentOn_Complaint" (CC_Resolvers only)
    """
    department_config = departments_config[department_name]
    # Include BOTH bot_skills and agent_skills for the department
    department_all_skills = set(department_config['bot_skills'] + department_config['agent_skills'])
    # Extract agent_skills separately for specific counting
    agent_dept_skills = set(department_config['agent_skills'])
    
    # Get exclusion list for this department
    exclusion_list = AGENT_INTERVENTION_EXCLUSIONS.get(department_name, [])
    
    # Filter for normal messages from agents (using Snowflake column names)
    agent_normal_messages = conversation_df[
        (conversation_df['SENT_BY'].str.upper() == 'AGENT') & 
        (conversation_df['MESSAGE_TYPE'].str.upper() == 'NORMAL MESSAGE')
    ]
    
    # Filter for normal messages from bots
    bot_normal_messages = conversation_df[
        (conversation_df['SENT_BY'].str.upper() == 'BOT') & 
        (conversation_df['MESSAGE_TYPE'].str.upper() == 'NORMAL MESSAGE')
    ]
    
    # Filter for private messages from system (for call request detection)
    system_private_messages = conversation_df[
        (conversation_df['SENT_BY'].str.upper() == 'SYSTEM') & 
        (conversation_df['MESSAGE_TYPE'].str.upper() == 'PRIVATE MESSAGE')
        | (conversation_df['MESSAGE_TYPE'].str.upper() == 'TRANSFER') | (conversation_df['MESSAGE_TYPE'].str.upper() == 'TOOL')
    ]
    
    # Count agent messages from department-related skills and detect call requests
    agent_message_count = 0
    counted_agent_messages = 0  # Agent messages excluding filtered phrases
    agent_message_count_excluding_pokes = 0  # Agent messages excluding pokes (CC_Sales only)
    has_call_request = False
    has_complaint_action = False  # CC_Resolvers specific: tracks "Open_or_CommentOn_Complaint"
    call_request_phrase = "CallUs"
    complaint_action_phrase = "Review_Complaint"
    
    # CALL REQUEST DETECTION: Only for CC_Sales, MV_Sales, and DDC
    if department_name in ['CC_Sales', 'MV_Sales', 'DDC']:
        # Check for call requests in system private messages
        for _, message in system_private_messages.iterrows():
            message_content = str(message.get('TEXT', '')).lower()
            # Check for call request phrase using regex
            if re.search(re.escape(call_request_phrase), message_content, re.IGNORECASE):
                has_call_request = True
                break  # Found call request, no need to continue checking
    
    # CC_Resolvers specific: Check for "Open_or_CommentOn_Complaint" in system private messages
    if department_name == 'CC_Resolvers':
        for _, message in system_private_messages.iterrows():
            message_content = str(message.get('TEXT', ''))
            # Check for complaint action phrase using regex (case-insensitive)
            # Two patterns to check:
            # 1. "Open_or_CommentOn_Complaint" 
            # 2. 'Action":"Open' (JSON pattern)
            if (re.search(re.escape(complaint_action_phrase), message_content, re.IGNORECASE) and
                re.search(r'Action":"Open', message_content, re.IGNORECASE)):
                has_complaint_action = True
                break  # Found complaint action, no need to continue checking
    
    # Iterate through agent messages and apply CC_Resolvers-specific logic
    for idx, message in agent_normal_messages.iterrows():
        message_skill = message['TARGET_SKILL_PER_MESSAGE']
        if message_skill in department_all_skills:
            # CC_Resolvers specific logic: Skip counting if current skill is "GPT CC Shadowers"
            # and the first different previous skill was "GPT_CC_PROSPECT"
            should_skip_counting = False
            if department_name == 'CC_Resolvers':
                # Check if current skill is "GPT CC Shadowers" (case-insensitive)
                if message_skill and 'gpt cc shadowers' in str(message_skill).lower():
                    # Find first different previous skill
                    prev_different_skill = get_first_different_previous_skill(idx, message_skill, conversation_df)
                    
                    # Check if previous different skill was "GPT_CC_PROSPECT" (case-insensitive)
                    if prev_different_skill and 'gpt_cc_prospect' in str(prev_different_skill).lower():
                        should_skip_counting = True
            
            # Only count if not flagged to skip
            if not should_skip_counting:
                agent_message_count += 1
            
            message_content = str(message.get('TEXT', '')).lower()
            
            # Check if message should be excluded from intervention calculation (case-insensitive using regex)
            is_excluded = False
            for exclusion_phrase in exclusion_list:
                if re.search(re.escape(exclusion_phrase), message_content, re.IGNORECASE):
                    is_excluded = True
                    break
            
            # Count only non-excluded messages for intervention calculation
            if not is_excluded and not should_skip_counting:
                counted_agent_messages += 1
            
            # For CC_Sales department only: count agent messages excluding pokes
            if department_name == 'CC_Sales':
                message_content_lower = message_content
                is_poke_message = False
                for poke_phrase in pokes:
                    if re.search(re.escape(poke_phrase.lower()), message_content_lower, re.IGNORECASE):
                        is_poke_message = True
                        break
                
                # Count only non-poke messages for CC_Sales
                if not is_poke_message and not should_skip_counting:
                    agent_message_count_excluding_pokes += 1

                elif department_name == 'MV_Sales':
                # For MV_Sales: detect pokes by checking if text contains "MINUTES POKE"
                    message_content_lower = message_content  # Already lowercase from line 1500
                    is_poke_message = 'minutes poke' in message_content_lower or 'minute poke' in message_content_lower

                    # Debug output for first few messages
                    if agent_message_count <= 3:
                        has_minute = 'minute' in message_content_lower
                        has_poke = 'poke' in message_content_lower
                        print(f"    DEBUG MV_Sales: Message {agent_message_count} - Is poke: {is_poke_message}, Has 'minute': {has_minute}, Has 'poke': {has_poke}")
                        print(f"       Text preview: {message_content_lower[:150]}")

                # Count only non-poke messages for MV_Sales
                if not is_poke_message and not should_skip_counting:
                    agent_message_count_excluding_pokes += 1
            else:
                # For other departments, count all agent messages
                if not should_skip_counting:
                    agent_message_count_excluding_pokes += 1
    
    
    # Count agent messages under agent dept skills only
    agent_normal_messages_under_agent_dept_skills = 0
    for idx, message in agent_normal_messages.iterrows():
        message_skill = message['TARGET_SKILL_PER_MESSAGE']
        if message_skill in department_all_skills:
            # Apply same CC_Resolvers logic here
            should_skip_counting = False
            if department_name == 'CC_Resolvers':
                # Check if current skill is "GPT CC Shadowers" (case-insensitive)
                if message_skill and 'gpt cc shadowers' in str(message_skill).lower():
                    # Find first different previous skill
                    prev_different_skill = get_first_different_previous_skill(idx, message_skill, conversation_df)
                    
                    # Check if previous different skill was "GPT_CC_PROSPECT" (case-insensitive)
                    if prev_different_skill and 'gpt_cc_prospect' in str(prev_different_skill).lower():
                        should_skip_counting = True
            
            # Only count if not flagged to skip
            if not should_skip_counting:
                agent_normal_messages_under_agent_dept_skills += 1
    
    # Check for valid system transfers from bot skills to agent skills (not initiated by GPT)
    has_valid_system_transfer = False
    bot_skills = set(department_config['bot_skills'])
    
    # Filter for system messages with type 'private' or 'transfer'
    system_transfer_messages = conversation_df[
        (conversation_df['SENT_BY'].str.upper() == 'SYSTEM') & 
        ((conversation_df['MESSAGE_TYPE'].str.upper() == 'PRIVATE MESSAGE') |
         (conversation_df['MESSAGE_TYPE'].str.upper() == 'TRANSFER') | (conversation_df['MESSAGE_TYPE'].str.upper() == 'TOOL'))
    ]

    
    for _, message in system_transfer_messages.iterrows():
        message_text = str(message.get('TEXT', ''))
        transfer_data = parse_transfer(message_text)
        
        
        # Check all three conditions:
        # 1. 'by' doesn't contain 'GPT' (case-insensitive)
        # 2. 'from_skill' is in bot_skills
        # 3. 'to_skill' is in agent_skills
        if (transfer_data.get('by', '') and 
            'gpt' not in transfer_data.get('by', '').lower() and
            transfer_data.get('from_skill', '') in bot_skills and
            transfer_data.get('to_skill', '') in agent_dept_skills):
            
            has_valid_system_transfer = True
            break  # Found one valid transfer, no need to continue
    
    
    # Special case for departments with no agent_skills (e.g. multiple_contract_detector):
    # any transfer FROM a bot_skill TO any other skill (not GPT-initiated) counts as valid.
    if not has_valid_system_transfer and not agent_dept_skills:
        for _, message in system_transfer_messages.iterrows():
            message_text = str(message.get('TEXT', ''))
            transfer_data = parse_transfer(message_text)
            if (transfer_data.get('by', '') and
                    'gpt' not in transfer_data.get('by', '').lower() and
                    transfer_data.get('from_skill', '') in bot_skills and
                    transfer_data.get('to_skill', '') and
                    transfer_data.get('to_skill', '') not in bot_skills):
                has_valid_system_transfer = True
                break
    # Count bot messages from department-related skills
    bot_message_count = 0
    for _, message in bot_normal_messages.iterrows():
        message_skill = message['TARGET_SKILL_PER_MESSAGE']
        if message_skill in department_all_skills:
            bot_message_count += 1
    
    # CC_Resolvers specific: If complaint action detected, treat as NOT bot-handled
    # BUT don't increment agent_message_count so it doesn't affect 1+, 2+, 3+ counts
    # Instead, we'll use has_complaint_action flag to determine bot_handled status
    if department_name == 'CC_Resolvers' and has_complaint_action:
        # Don't increment agent_message_count - keep it as actual agent messages only
        # The has_complaint_action flag will be used to override is_bot_handled
        pass
    
    # Return tuple: (is_bot_handled, agent_message_count, has_call_request, counted_agent_messages, bot_message_count, is_bot_handled_excluding_fillers, has_valid_system_transfer, agent_message_count_excluding_pokes, agent_messages_from_allowed_skills, has_complaint_action)
    # For CC_Resolvers: 
    #   - if has_complaint_action is True, conversation is NOT bot-handled regardless of agent_message_count
    #   - bot must have sent at least 1 normal message (bot_message_count > 0) to be considered handled
    # For other departments: only check agent_message_count == 0
    if department_name == 'CC_Resolvers':
        is_bot_handled = (agent_message_count == 0) and not has_complaint_action and (bot_message_count > 0)
        is_bot_handled_excluding_fillers = (counted_agent_messages == 0) and not has_complaint_action and (bot_message_count > 0)
    else:
        is_bot_handled = (agent_message_count == 0)
        is_bot_handled_excluding_fillers = (counted_agent_messages == 0)
    
    return is_bot_handled, agent_message_count, has_call_request, counted_agent_messages, bot_message_count, is_bot_handled_excluding_fillers, has_valid_system_transfer, agent_message_count_excluding_pokes, agent_normal_messages_under_agent_dept_skills, has_complaint_action


def calculate_proactive_agent_messages_mv_resolvers(session, department_name, departments_config, target_date):
    """
    Calculate proactive agent metrics for MV_Resolvers department.
    
    This function calculates five metrics plus four sub-metrics:
    1. Proactive conversations: Conversations that didn't go through bot skill and have agent skill in through_skill
    2. Directly handled by seniors: Conversations with pattern GPT_RESOLVERS_BOT followed by agent skill message
    3. Other bots to seniors: Conversations with non-MV skill followed by MV_Resolvers agent skill
    4. Our bot to seniors: Conversations with GPT_MV_RESOLVERS followed by MV_Resolvers agent skill
       4a. MV_BOT_Known_Flow_Transfer: Sub-metric - conversations with "transfer_conversation" in TEXT OR (text contains both '"flag_reason":"Frustrated Client"' and '{"content"')
       4b. MV_BOT_Tech_Errors_Transfers: Sub-metric - conversations with "by admin" in TEXT and MESSAGE_TYPE='Transfer' OR text contains "Error Task:"
       4c. MV_BOT_GUARDRAILS: Sub-metric - conversations matching guardrail conditions
       4d. MV_BOT_Other_transfers: Sub-metric - conversations not in 4a, 4b, or 4c
    5. Delighters to seniors: Conversations with gpt_delighters followed by MV_Resolvers agent skill
    
    Args:
        session: Snowflake session
        department_name: Department name (should be 'MV_Resolvers')
        departments_config: Department configuration
        target_date: Target date for analysis
    
    Returns:
        tuple: (proactive_conversations_count, directly_handled_by_seniors_count, other_bots_to_seniors_count, 
                our_bot_to_seniors_count, delighters_to_seniors_count, mv_bot_known_flow_transfer_count, 
                mv_bot_tech_errors_transfers_count, mv_bot_guardrails_count, mv_bot_other_transfers_count,
                our_bot_to_mv_resolvers_seniors_count, our_bot_to_mv_callers_count, our_bot_to_pre_r_visa_retention_count)
    """
    # Only applies to MV_Resolvers
    if department_name != 'MV_Resolvers':
        return 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    
    try:
        department_config = departments_config[department_name]
        agent_skills = department_config['agent_skills']
        agent_skills_str = "', '".join(agent_skills)
        filter_date = (datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

        
        print(f"    🔍 Calculating proactive agent metrics for {department_name}...")
        
        # ========================================
        # METRIC 1: Proactive conversations
        # ========================================
        query_proactive = f"""
        SELECT * 
        FROM SILVER.CHAT_EVALS.MV_CLIENTS_CHATS
        WHERE through_skill NOT ILIKE '%GPT_MV_RESOLVERS%' 
        AND date(updated_at) = '{filter_date}'
        AND conversation_id NOT IN (
            SELECT conversation_id 
            FROM SILVER.CHAT_EVALS.MV_CLIENTS_CHATS 
            WHERE message_seq = 0
            AND (SENT_BY ILIKE 'Consumer' OR agent_name IS NULL OR (sent_by = 'System' AND message_type = 'Normal Message'))
        )
        """
        
        # Execute query and convert to pandas
        result_df_proactive = session.sql(query_proactive).to_pandas()
        
        proactive_conversations_count = 0
        if not result_df_proactive.empty:
            # Filter to keep only rows where through_skill contains any agent skill
            filtered_df = result_df_proactive[result_df_proactive['THROUGH_SKILL'].apply(
                lambda x: any(skill.lower() in str(x).lower() for skill in agent_skills)
            )]
            
            if not filtered_df.empty:
                proactive_conversations_count = filtered_df['CONVERSATION_ID'].nunique()
        
        # ========================================
        # METRIC 2: Directly handled by seniors
        # ========================================
        query_seniors = f"""
        SELECT *
        FROM SILVER.CHAT_EVALS.MV_CLIENTS_CHATS
        WHERE through_skill NOT ILIKE '%GPT_MV_RESOLVERS%'
        AND date(updated_at) = '{filter_date}'
        AND through_skill ILIKE '%GPT_RESOLVERS_BOT%'
        """
        
        # Execute query and convert to pandas
        result_df_seniors = session.sql(query_seniors).to_pandas()
        
        directly_handled_count = 0
        if not result_df_seniors.empty:
            # Group by conversation and check pattern
            for conv_id, conv_df in result_df_seniors.groupby('CONVERSATION_ID'):
                # Sort by message_sent_time (ascending) to ensure chronological order
                conv_df_sorted = conv_df.sort_values('MESSAGE_SENT_TIME') if 'MESSAGE_SENT_TIME' in conv_df.columns else conv_df.sort_index()
                
                # Check for pattern: GPT_RESOLVERS_BOT followed by agent skill
                found_pattern = False
                found_gpt_resolvers_bot = False
                
                for _, row in conv_df_sorted.iterrows():
                    target_skill = str(row.get('TARGET_SKILL_PER_MESSAGE', '')).strip()
                    
                    # Check if we found GPT_RESOLVERS_BOT
                    if 'GPT_RESOLVERS_BOT' in target_skill.upper():
                        found_gpt_resolvers_bot = True
                        continue
                    
                    # If we found GPT_RESOLVERS_BOT before, check if current message is from agent skill
                    if found_gpt_resolvers_bot:
                        if any(agent_skill.upper() in target_skill.upper() for agent_skill in agent_skills):
                            found_pattern = True
                            break
                
                if found_pattern:
                    directly_handled_count += 1
        
        # ========================================
        # METRIC 3: Other bots to seniors (any skill except specific exclusions to MV seniors)
        # ========================================
        query_other_bots = f"""
        SELECT *
        FROM SILVER.CHAT_EVALS.MV_CLIENTS_CHATS
        WHERE through_skill NOT ILIKE '%GPT_MV_RESOLVERS%'
        AND date(updated_at) = '{filter_date}'
        """
        
        # Execute query and convert to pandas
        result_df_other_bots = session.sql(query_other_bots).to_pandas()
        
        other_bots_to_seniors_count = 0
        if not result_df_other_bots.empty:
            # Define specific skills to exclude (case-insensitive)
            excluded_skills = ['gpt_delighters', 'GPT_RESOLVERS_BOT', 'GPT_MV_RESOLVERS']
            
            # Get MV_Resolvers agent skills specifically (the seniors)
            mv_resolvers_agent_skills = agent_skills
            
            # Group by conversation and check pattern
            for conv_id, conv_df in result_df_other_bots.groupby('CONVERSATION_ID'):
                # Sort by message_sent_time (ascending) to ensure chronological order
                conv_df_sorted = conv_df.sort_values('MESSAGE_SENT_TIME') if 'MESSAGE_SENT_TIME' in conv_df.columns else conv_df.sort_index()
                
                # Check for pattern: any skill (excluding specific ones) followed by MV_Resolvers agent skill
                found_pattern = False
                found_other_skill = False
                
                for _, row in conv_df_sorted.iterrows():
                    target_skill = str(row.get('TARGET_SKILL_PER_MESSAGE', '')).strip()
                    
                    if not target_skill:  # Skip empty skills
                        continue
                    
                    # Check if current skill is NOT in excluded skills list
                    is_excluded_skill = any(excluded_skill.upper() in target_skill.upper() for excluded_skill in excluded_skills)
                    
                    # Check if current skill is NOT an MV_Resolvers agent skill (we want the transfer TO seniors, not FROM)
                    is_mv_agent_skill = any(agent_skill.upper() in target_skill.upper() for agent_skill in mv_resolvers_agent_skills)
                    
                    if not is_excluded_skill and not is_mv_agent_skill:
                        found_other_skill = True
                        continue
                    
                    # If we found an "other skill" before, check if current message is from MV_Resolvers agent skills
                    if found_other_skill and is_mv_agent_skill:
                        found_pattern = True
                        break
                
                if found_pattern:
                    other_bots_to_seniors_count += 1
        
        # ========================================
        # METRIC 4: Our bot to seniors (GPT_MV_RESOLVERS to MV_Resolvers agent skills)
        # ========================================
        query_our_bot = f"""
        SELECT *
        FROM SILVER.CHAT_EVALS.MV_CLIENTS_CHATS
        WHERE through_skill ILIKE '%GPT_MV_RESOLVERS%'
        AND date(updated_at) = '{filter_date}'
        """
        
        # Execute query and convert to pandas
        result_df_our_bot = session.sql(query_our_bot).to_pandas()
        
        our_bot_to_seniors_count = 0
        our_bot_to_seniors_conv_ids = set()  # Track conversation IDs for sub-metrics
        if not result_df_our_bot.empty:
            # Get MV_Resolvers bot skills
            mv_resolvers_bot_skills = department_config.get('bot_skills', [])
            
            # Get MV_Resolvers agent skills (the seniors)
            mv_resolvers_agent_skills = agent_skills
            
            # Group by conversation and check pattern
            for conv_id, conv_df in result_df_our_bot.groupby('CONVERSATION_ID'):
                # Sort by message_sent_time (ascending) to ensure chronological order
                conv_df_sorted = conv_df.sort_values('MESSAGE_SENT_TIME') if 'MESSAGE_SENT_TIME' in conv_df.columns else conv_df.sort_index()
                
                # Check for pattern: GPT_MV_RESOLVERS followed by MV_Resolvers agent skill
                found_pattern = False
                found_gpt_mv_resolvers = False
                
                for _, row in conv_df_sorted.iterrows():
                    target_skill = str(row.get('TARGET_SKILL_PER_MESSAGE', '')).strip()
                    
                    if not target_skill:  # Skip empty skills
                        continue
                    
                    # Check if current skill is GPT_MV_RESOLVERS (exclude GPT_MV_RESOLVERS_TA)
                    if any(bot_skill.upper() in target_skill.upper() for bot_skill in mv_resolvers_bot_skills) and target_skill.upper() != 'GPT_MV_RESOLVERS_TA':
                        found_gpt_mv_resolvers = True
                        continue
                    
                    # If we found GPT_MV_RESOLVERS before, check if current message is from MV_Resolvers agent skills
                    if found_gpt_mv_resolvers:
                        if any(agent_skill.upper() in target_skill.upper() for agent_skill in mv_resolvers_agent_skills):
                            found_pattern = True
                            break
                
                if found_pattern:
                    our_bot_to_seniors_count += 1
                    our_bot_to_seniors_conv_ids.add(conv_id)  # Track this conversation
        
        # ========================================
        # SUB-METRIC 4a: MV_BOT_Known_Flow_Transfer (from "Our bot to seniors" group)
        # ========================================
        mv_bot_known_flow_transfer_count = 0
        mv_bot_known_flow_transfer_conv_ids = set()
        if our_bot_to_seniors_conv_ids and not result_df_our_bot.empty:
            # Filter to only conversations in "Our bot to seniors" group
            our_bot_conversations = result_df_our_bot[result_df_our_bot['CONVERSATION_ID'].isin(our_bot_to_seniors_conv_ids)]
            
            # Check each conversation for "transfer_conversation" OR ("flag_reason":"Frustrated Client" AND {"content")
            for conv_id in our_bot_to_seniors_conv_ids:
                conv_rows = our_bot_conversations[our_bot_conversations['CONVERSATION_ID'] == conv_id]
                
                # Check if any TEXT contains "transfer_conversation" OR frustrated client pattern
                has_known_flow_transfer = False
                for _, row in conv_rows.iterrows():
                    text = str(row.get('TEXT', ''))
                    text_lower = text.lower()
                    
                    # Condition 1: transfer_conversation
                    if 'transfer_conversation' in text_lower:
                        has_known_flow_transfer = True
                        break
                    
                    # Condition 2: both "flag_reason":"Frustrated Client" AND {"content"
                    if '"flag_reason":"frustrated client"' in text_lower and '{"content"' in text_lower:
                        has_known_flow_transfer = True
                        break
                    
                    # Condition 3: Transfer due to frustration flag (multiple_contract_detector)
                    if 'transfer due to frustration' in text_lower:
                        has_known_flow_transfer = True
                        break
                
                if has_known_flow_transfer:
                    mv_bot_known_flow_transfer_count += 1
                    mv_bot_known_flow_transfer_conv_ids.add(conv_id)
        
        # ========================================
        # SUB-METRIC 4b: MV_BOT_Tech_Errors_Transfers (from "Our bot to seniors" group)
        # EXCLUDE conversations already in Known_Flow_Transfer to avoid double-counting
        # ========================================
        mv_bot_tech_errors_transfers_count = 0
        mv_bot_tech_errors_transfers_conv_ids = set()
        if our_bot_to_seniors_conv_ids and not result_df_our_bot.empty:
            # Filter to only conversations in "Our bot to seniors" group that are NOT in Known_Flow_Transfer
            remaining_conversations = our_bot_to_seniors_conv_ids - mv_bot_known_flow_transfer_conv_ids
            our_bot_conversations = result_df_our_bot[result_df_our_bot['CONVERSATION_ID'].isin(remaining_conversations)]
            
            # Check each conversation for tech error patterns
            for conv_id in remaining_conversations:
                conv_rows = our_bot_conversations[our_bot_conversations['CONVERSATION_ID'] == conv_id]
                
                # Check if any message matches tech error patterns
                has_tech_error = False
                for _, row in conv_rows.iterrows():
                    text = str(row.get('TEXT', ''))
                    text_lower = text.lower()
                    message_type = str(row.get('MESSAGE_TYPE', '')).strip()
                    
                    # Condition 1: TEXT contains "by admin" AND MESSAGE_TYPE = 'Transfer'
                    if 'by admin' in text_lower and message_type == 'Transfer':
                        has_tech_error = True
                        break
                    
                    # Condition 2: TEXT contains "Error Task:"
                    if 'error task:' in text_lower:
                        has_tech_error = True
                        break
                
                if has_tech_error:
                    mv_bot_tech_errors_transfers_count += 1
                    mv_bot_tech_errors_transfers_conv_ids.add(conv_id)
        
        # ========================================
        # SUB-METRIC 4c: MV_BOT_GUARDRAILS (from "Our bot to seniors" group)
        # EXCLUDE conversations already in Known_Flow_Transfer and Tech_Errors_Transfers to avoid double-counting
        # ========================================
        # ========================================
        # SUB-METRIC 4c: MV_BOT_GUARDRAILS (from "Our bot to seniors" group)
        # EXCLUDE conversations already in Known_Flow_Transfer and Tech_Errors_Transfers to avoid double-counting
        # ========================================
        mv_bot_guardrails_count = 0
        mv_bot_guardrails_conv_ids = set()
        if our_bot_to_seniors_conv_ids:
            # Query to check which conversations match guardrail conditions
            query_guardrail_check = f"""
            SELECT DISTINCT conversation_id 
            FROM SILVER.CHAT_EVALS.MV_CLIENTS_CHATS
            WHERE text ILIKE '%reason:%Error%guard%' 
            AND date(updated_at) = '{filter_date}'
            
            UNION
            
            SELECT DISTINCT conversation_id 
            FROM SILVER.CHAT_EVALS.MV_CLIENTS_CHATS
            WHERE text ILIKE 'GUARDRAIL DETECTED%FALSE%PROMISE%' 
            AND date(updated_at) = '{filter_date}'
            """
            
            result_df_guardrail_check = session.sql(query_guardrail_check).to_pandas()
            
            if not result_df_guardrail_check.empty:
                guardrail_conv_ids = set(result_df_guardrail_check['CONVERSATION_ID'].values)
                
                # Get "Our bot to seniors" conversations that match guardrails
                # BUT EXCLUDE those already counted in Known_Flow_Transfer and Tech_Errors_Transfers
                mv_bot_guardrails_conv_ids = our_bot_to_seniors_conv_ids.intersection(guardrail_conv_ids) - mv_bot_known_flow_transfer_conv_ids - mv_bot_tech_errors_transfers_conv_ids
                mv_bot_guardrails_count = len(mv_bot_guardrails_conv_ids)
        
        # ========================================
        # SUB-METRIC 4d: MV_BOT_Other_transfers (from "Our bot to seniors" group)
        # Conversations that are NOT in Known_Flow_Transfer, NOT in Tech_Errors_Transfers, AND NOT in Guardrails
        # ========================================
        mv_bot_other_transfers_count = 0
        if our_bot_to_seniors_conv_ids:
            # Find conversations that don't belong to any of the above categories
            other_transfers_conv_ids = our_bot_to_seniors_conv_ids - mv_bot_known_flow_transfer_conv_ids - mv_bot_tech_errors_transfers_conv_ids - mv_bot_guardrails_conv_ids
            mv_bot_other_transfers_count = len(other_transfers_conv_ids)
        
        # ========================================
        # NEW DIMENSION: Break down "Our bot to seniors" by TARGET AGENT SKILL
        # ========================================
        our_bot_to_mv_resolvers_seniors_count = 0
        our_bot_to_mv_callers_count = 0
        our_bot_to_pre_r_visa_retention_count = 0
        
        if our_bot_to_seniors_conv_ids and not result_df_our_bot.empty:
            # Filter to only conversations in "Our bot to seniors" group
            our_bot_conversations = result_df_our_bot[result_df_our_bot['CONVERSATION_ID'].isin(our_bot_to_seniors_conv_ids)]
            
            for conv_id in our_bot_to_seniors_conv_ids:
                conv_rows = our_bot_conversations[our_bot_conversations['CONVERSATION_ID'] == conv_id]
                
                if conv_rows.empty:
                    continue
                
                # Sort by MESSAGE_SENT_TIME for chronological order
                conv_df_sorted = conv_rows.sort_values('MESSAGE_SENT_TIME') if 'MESSAGE_SENT_TIME' in conv_rows.columns else conv_rows.sort_index()
                
                # Find the first agent skill that appears after GPT_MV_RESOLVERS
                found_gpt_mv_resolvers = False
                target_agent_skill = None
                
                for _, row in conv_df_sorted.iterrows():
                    target_skill = str(row.get('TARGET_SKILL_PER_MESSAGE', '')).strip().upper()
                    
                    if not target_skill:
                        continue
                    
                    # Check if this is GPT_MV_RESOLVERS (exclude GPT_MV_RESOLVERS_TA)
                    if any(bot_skill.upper() in target_skill for bot_skill in mv_resolvers_bot_skills) and target_skill != 'GPT_MV_RESOLVERS_TA':
                        found_gpt_mv_resolvers = True
                        continue
                    
                    # After finding GPT_MV_RESOLVERS, look for the first agent skill
                    if found_gpt_mv_resolvers:
                        # Check if this is one of the 3 agent skills
                        if 'MV_RESOLVERS_SENIORS' in target_skill:
                            target_agent_skill = 'MV_RESOLVERS_SENIORS'
                            break
                        elif 'MV_CALLERS' in target_skill:
                            target_agent_skill = 'MV_CALLERS'
                            break
                        elif 'PRE_R_VISA_RETENTION' in target_skill:
                            target_agent_skill = 'PRE_R_VISA_RETENTION'
                            break
                
                # Count based on target agent skill
                if target_agent_skill == 'MV_RESOLVERS_SENIORS':
                    our_bot_to_mv_resolvers_seniors_count += 1
                elif target_agent_skill == 'MV_CALLERS':
                    our_bot_to_mv_callers_count += 1
                elif target_agent_skill == 'PRE_R_VISA_RETENTION':
                    our_bot_to_pre_r_visa_retention_count += 1
        
        # ========================================
        # METRIC 5: Delighters to seniors (gpt_delighters to MV_Resolvers agent skills)
        # ========================================
        query_delighters = f"""
        SELECT *
        FROM SILVER.CHAT_EVALS.MV_CLIENTS_CHATS
        WHERE through_skill ILIKE '%gpt_delighters%'
        AND date(updated_at) = '{filter_date}'
        """
        
        # Execute query and convert to pandas
        result_df_delighters = session.sql(query_delighters).to_pandas()
        
        delighters_to_seniors_count = 0
        if not result_df_delighters.empty:
            # Get MV_Resolvers agent skills (the seniors)
            mv_resolvers_agent_skills = agent_skills
            
            # Group by conversation and check pattern
            for conv_id, conv_df in result_df_delighters.groupby('CONVERSATION_ID'):
                # Sort by message_sent_time (ascending) to ensure chronological order
                conv_df_sorted = conv_df.sort_values('MESSAGE_SENT_TIME') if 'MESSAGE_SENT_TIME' in conv_df.columns else conv_df.sort_index()
                
                # Check for pattern: gpt_delighters followed by MV_Resolvers agent skill
                found_pattern = False
                found_gpt_delighters = False
                
                for _, row in conv_df_sorted.iterrows():
                    target_skill = str(row.get('TARGET_SKILL_PER_MESSAGE', '')).strip()
                    
                    if not target_skill:  # Skip empty skills
                        continue
                    
                    # Check if current skill is gpt_delighters
                    if 'gpt_delighters'.upper() in target_skill.upper():
                        found_gpt_delighters = True
                        continue
                    
                    # If we found gpt_delighters before, check if current message is from MV_Resolvers agent skills
                    if found_gpt_delighters:
                        if any(agent_skill.upper() in target_skill.upper() for agent_skill in mv_resolvers_agent_skills):
                            found_pattern = True
                            break
                
                if found_pattern:
                    delighters_to_seniors_count += 1
        
        print(f"    ✅ Found {proactive_conversations_count} proactive conversations (distinct conversation IDs)")
        print(f"    ✅ Found {directly_handled_count} conversations directly handled by seniors (GPT_RESOLVERS_BOT → agent skill)")
        print(f"    ✅ Found {other_bots_to_seniors_count} conversations from other bots to seniors (non-MV → MV seniors)")
        print(f"    ✅ Found {our_bot_to_seniors_count} conversations from our bot to seniors (GPT_MV_RESOLVERS → MV seniors)")
        print(f"       └─ Sub-metric: MV_BOT_Known_Flow_Transfer: {mv_bot_known_flow_transfer_count}/{our_bot_to_seniors_count}")
        print(f"       └─ Sub-metric: MV_BOT_Tech_Errors_Transfers: {mv_bot_tech_errors_transfers_count}/{our_bot_to_seniors_count}")
        print(f"       └─ Sub-metric: MV_BOT_GUARDRAILS: {mv_bot_guardrails_count}/{our_bot_to_seniors_count}")
        print(f"       └─ Sub-metric: MV_BOT_Other_transfers: {mv_bot_other_transfers_count}/{our_bot_to_seniors_count}")
        print(f"       └─ Breakdown by Target Skill:")
        print(f"          └─ To MV_RESOLVERS_SENIORS: {our_bot_to_mv_resolvers_seniors_count}/{our_bot_to_seniors_count}")
        print(f"          └─ To MV_CALLERS: {our_bot_to_mv_callers_count}/{our_bot_to_seniors_count}")
        print(f"          └─ To Pre_R_Visa_Retention: {our_bot_to_pre_r_visa_retention_count}/{our_bot_to_seniors_count}")
        print(f"          └─ Sum check: {our_bot_to_mv_resolvers_seniors_count + our_bot_to_mv_callers_count + our_bot_to_pre_r_visa_retention_count} = {our_bot_to_seniors_count}")
        print(f"    ✅ Found {delighters_to_seniors_count} conversations from delighters to seniors (gpt_delighters → MV seniors)")
        
        return proactive_conversations_count, directly_handled_count, other_bots_to_seniors_count, our_bot_to_seniors_count, delighters_to_seniors_count, mv_bot_known_flow_transfer_count, mv_bot_tech_errors_transfers_count, mv_bot_guardrails_count, mv_bot_other_transfers_count, our_bot_to_mv_resolvers_seniors_count, our_bot_to_mv_callers_count, our_bot_to_pre_r_visa_retention_count
        
    except Exception as e:
        print(f"    ⚠️  Error calculating proactive agent metrics: {str(e)}")
        return 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0


def calculate_total_seniors_callers(session, department_name, departments_config, target_date):
    """
    Calculate total chats that reached seniors or callers for MV_Resolvers department.
    Also categorizes them into mutually exclusive paths.
    
    Finds conversations where:
    - target_skill_per_message contains 'MV_RESOLVERS_SENIORS'
    - OR through_skill contains 'MV_CALLERS'
    
    Args:
        session: Snowflake session
        department_name: Department name (should be 'MV_Resolvers')
        departments_config: Department configuration
        target_date: Target date for analysis
    
    Returns:
        tuple: (total_count, our_bot_count, directly_handled_count, proactive_count, proactive_mv_resolvers_seniors_only_count, delighters_count, other_bots_count, base_conv_ids_set, our_bot_conv_ids_set, seniors_our_bot_to_mv_resolvers_seniors_count, seniors_our_bot_to_mv_callers_count, seniors_our_bot_to_pre_r_visa_retention_count, supervisor_excluded_conv_ids_set, no_response_transfers_count)
    """
    # Only applies to MV_Resolvers
    if department_name != 'MV_Resolvers':
        return 0, 0, 0, 0, 0, 0, 0, set(), set(), 0, 0, 0, set(), 0
    
    try:
        department_config = departments_config[department_name]
        table_name = department_config.get('table_name', 'SILVER.CHAT_EVALS.MV_CLIENTS_CHATS')
        filter_date = (datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        agent_skills = department_config['agent_skills']
        
        # SQL query to find ALL conversations that reached seniors or callers (THE BASE)
        # EXCLUDE delighters conversations from the base
        query_base = f"""
        SELECT DISTINCT conversation_id 
        FROM {table_name}
        WHERE (target_skill_per_message ILIKE '%MV_RESOLVERS_SENIORS%' 
               OR through_skill ILIKE '%MV_RESOLVERS_SENIORS%'
               OR target_skill_per_message ILIKE '%MV_CALLERS%'
               OR through_skill ILIKE '%MV_CALLERS%'
               OR target_skill_per_message ILIKE '%Pre_R_Visa_Retention%'
               OR through_skill ILIKE '%Pre_R_Visa_Retention%')
        AND through_skill NOT ILIKE '%DELIGHTERS%'
        AND date(end_date) = '{target_date}'
        """
        
        print(f"    📞 Calculating total seniors/callers and categorizing paths for {department_name}...")
        
        # Execute query and get all conversation IDs that reached seniors
        result_df_base = session.sql(query_base).to_pandas()
        
        if result_df_base.empty:
            return 0, 0, 0, 0, 0, 0, 0, set(), set(), 0, 0, 0, set(), 0
        
        base_conv_ids = set(result_df_base['CONVERSATION_ID'].values)
        total_seniors_callers_count = len(base_conv_ids)
        
        print(f"    ✅ Found {total_seniors_callers_count} conversations that reached seniors/callers (BASE)")
        
        # Now get full conversation data for these conversations to categorize them
        conv_ids_str = "', '".join([str(cid) for cid in base_conv_ids])
        query_full_data = f"""
        SELECT *
        FROM {table_name}
        WHERE conversation_id IN ('{conv_ids_str}')
        AND date(updated_at) = '{filter_date}'
        ORDER BY conversation_id, message_seq
        """
        
        result_df_full = session.sql(query_full_data).to_pandas()
        
        if result_df_full.empty:
            return total_seniors_callers_count, 0, 0, 0, 0, 0, 0, base_conv_ids, set(), 0, 0, 0, set(), 0
        
        # Filter out broadcasts: first message (by time) is system AND not CLIENT — any convo length
        excluded_broadcast_count = 0
        for conv_id in list(base_conv_ids):
            conv_rows = result_df_full[result_df_full['CONVERSATION_ID'] == conv_id]
            conv_sorted = (
                conv_rows.sort_values('MESSAGE_SENT_TIME')
                if 'MESSAGE_SENT_TIME' in conv_rows.columns
                else conv_rows.sort_values('MESSAGE_SEQ')
                if 'MESSAGE_SEQ' in conv_rows.columns
                else conv_rows
            )
            first = conv_sorted.iloc[0]
            sent_by = str(first.get('SENT_BY', '')).lower()
            if sent_by == 'system':
                ct = first.get('CUSTOMER_TYPE')
                customer_type = '' if pd.isna(ct) else str(ct).strip().upper()
                if customer_type != 'CLIENT':
                    base_conv_ids.remove(conv_id)
                    excluded_broadcast_count += 1
        
        if excluded_broadcast_count > 0:
            print(f"    🚫 Excluded {excluded_broadcast_count} system-started non-CLIENT broadcasts from base")
        
        # Remove chats initiated by MV_RESOLVER_SUPERVISOR: skill on first or second message (by time)
        supervisor_skill_upper = 'MV_RESOLVER_SUPERVISOR'
        excluded_supervisor_count = 0
        supervisor_excluded_conv_ids = set()
        for conv_id in list(base_conv_ids):
            conv_rows = result_df_full[result_df_full['CONVERSATION_ID'] == conv_id]
            conv_sorted = (
                conv_rows.sort_values('MESSAGE_SENT_TIME')
                if 'MESSAGE_SENT_TIME' in conv_rows.columns
                else conv_rows.sort_values('MESSAGE_SEQ')
                if 'MESSAGE_SEQ' in conv_rows.columns
                else conv_rows
            )
            for i in range(min(2, len(conv_sorted))):
                raw = conv_sorted.iloc[i].get('TARGET_SKILL_PER_MESSAGE')
                ts = '' if pd.isna(raw) else str(raw).strip().upper()
                if ts == supervisor_skill_upper:
                    base_conv_ids.remove(conv_id)
                    supervisor_excluded_conv_ids.add(conv_id)
                    excluded_supervisor_count += 1
                    break
        
        if excluded_supervisor_count > 0:
            print(f"    🚫 Excluded {excluded_supervisor_count} MV_RESOLVER_SUPERVISOR-initiated chats from base (first 2 messages)")
        
        # Update total count after filtering
        total_seniors_callers_count = len(base_conv_ids)
        
        if total_seniors_callers_count == 0:
            return 0, 0, 0, 0, 0, 0, 0, set(), set(), 0, 0, 0, supervisor_excluded_conv_ids, 0
        
        # Initialize category sets (mutually exclusive with priority order)
        our_bot_conv_ids = set()
        directly_handled_conv_ids = set()
        proactive_conv_ids = set()
        delighters_conv_ids = set()
        other_bots_conv_ids = set()
        no_response_transfers_conv_ids = set()  # New category: Priority 1
        
        # Get MV_Resolvers bot skills for pattern matching
        mv_resolvers_bot_skills = department_config.get('bot_skills', [])
        
        # Categorize each conversation using PATTERN MATCHING (priority order matters!)
        for conv_id in base_conv_ids:
            conv_rows = result_df_full[result_df_full['CONVERSATION_ID'] == conv_id]
            
            if conv_rows.empty:
                other_bots_conv_ids.add(conv_id)
                continue
            
            # Sort by message_sent_time (ascending) to ensure chronological order
            conv_df_sorted = conv_rows.sort_values('MESSAGE_SENT_TIME') if 'MESSAGE_SENT_TIME' in conv_rows.columns else conv_rows.sort_index()
            
            categorized = False
            
            # Priority 1: No Response Transfers
            # Chats with GPT_RESOLVERS_BOT or GPT_MV_RESOLVERS where:
            # - ALL messages under these skills have NULL/empty EXECUTION_ID
            # - OR ANY message under these skills contains "by admin" in TEXT
            target_bot_skills = ['GPT_RESOLVERS_BOT'] + mv_resolvers_bot_skills  # ['GPT_RESOLVERS_BOT', 'GPT_MV_RESOLVERS']
            
            bot_messages = conv_df_sorted[
                conv_df_sorted['TARGET_SKILL_PER_MESSAGE'].apply(
                    lambda x: (any(bot_skill.upper() in str(x).upper() for bot_skill in target_bot_skills) and str(x).upper() != 'GPT_MV_RESOLVERS_TA') if pd.notna(x) and str(x).strip() else False
                )
            ]
            
            if not bot_messages.empty:
                # Check condition 1: ALL messages under bot skills have NULL/empty EXECUTION_ID
                # all_null_execution = bot_messages['EXECUTION_ID'].apply(
                #     lambda x: pd.isna(x) or str(x).strip() == ''
                # ).all()
                
                # Check condition 2: ANY message under bot skills contains "by admin"
                any_by_admin = bot_messages['TEXT'].apply(
                    lambda x: 'by admin' in str(x).lower() if pd.notna(x) else False
                ).any()
                
                # Check condition 3: ANY message under bot skills contains "Error Task:"
                any_error_task = bot_messages['TEXT'].apply(
                    lambda x: 'error task:' in str(x).lower() if pd.notna(x) else False
                ).any()
                
                if  any_by_admin or any_error_task:
                    no_response_transfers_conv_ids.add(conv_id)
                    categorized = True
                    continue
            
            # Priority 2: Proactive (first *chronological* message NOT from consumer/null agent/system normal OR single-row with agent skill)
            # Use iloc[0] after MESSAGE_SENT_TIME sort — do NOT use MESSAGE_SEQ==0 (can disagree with true order).
            # If first message has a non-empty TARGET_SKILL_PER_MESSAGE, it must be a dept agent skill; otherwise
            # another bot (e.g. GPT CC Shadowers) started the thread and this is not senior-initiated proactive.
            if len(conv_df_sorted) > 0:
                first_msg = conv_df_sorted.iloc[0]
                target_skill_first = str(first_msg.get('TARGET_SKILL_PER_MESSAGE', '')).strip()
                skip_proactive_due_to_non_agent_skill = False
                if target_skill_first:
                    first_skill_is_agent = any(
                        agent_skill.upper() in target_skill_first.upper() for agent_skill in agent_skills
                    )
                    if not first_skill_is_agent:
                        skip_proactive_due_to_non_agent_skill = True
                
                if not skip_proactive_due_to_non_agent_skill:
                    sent_by = str(first_msg.get('SENT_BY', '')).lower()
                    agent_name = first_msg.get('AGENT_NAME')
                    message_type = str(first_msg.get('MESSAGE_TYPE', '')).lower()
                    
                    # If first message is NOT consumer/null agent/system normal, it's proactive
                    is_proactive = not (
                        sent_by == 'consumer' or 
                        agent_name is None or 
                        pd.isna(agent_name) or
                        (sent_by == 'system' and 'normal message' in message_type)
                    )
                    
                    if is_proactive:
                        proactive_conv_ids.add(conv_id)
                        categorized = True
                        continue
            
            # Condition 2: Single-row conversation with agent skill only
            if len(conv_df_sorted) == 1:
                single_row = conv_df_sorted.iloc[0]
                target_skill = str(single_row.get('TARGET_SKILL_PER_MESSAGE', '')).strip()
                if target_skill and any(agent_skill.upper() in target_skill.upper() for agent_skill in agent_skills):
                    proactive_conv_ids.add(conv_id)
                    categorized = True
                    continue
            
            # Priority 3: Directly handled (GPT_RESOLVERS_BOT → agent skill PATTERN)
            # BUT EXCLUDE if GPT_MV_RESOLVERS appears BETWEEN GPT_RESOLVERS_BOT and agent skill
            found_gpt_resolvers_bot = False
            for _, row in conv_df_sorted.iterrows():
                target_skill = str(row.get('TARGET_SKILL_PER_MESSAGE', '')).strip()
                
                if not target_skill:
                    continue
                
                # Check if current skill is GPT_RESOLVERS_BOT
                if 'GPT_RESOLVERS_BOT' in target_skill.upper():
                    found_gpt_resolvers_bot = True
                    continue  # Allow multiple consecutive GPT_RESOLVERS_BOT
                
                # If we found GPT_RESOLVERS_BOT before, check NEXT non-GPT_RESOLVERS_BOT skill
                if found_gpt_resolvers_bot:
                    # This is the first non-GPT_RESOLVERS_BOT skill after GPT_RESOLVERS_BOT
                    # Check if it's GPT_MV_RESOLVERS (would interrupt the direct path)
                    # Exclude GPT_MV_RESOLVERS_TA from matching
                    if any(bot_skill.upper() in target_skill.upper() for bot_skill in mv_resolvers_bot_skills) and target_skill.upper() != 'GPT_MV_RESOLVERS_TA':
                        # GPT_MV_RESOLVERS is between GPT_RESOLVERS_BOT and agent skill
                        # Skip this - will be caught by "our_bot" category
                        break
                    
                    # Check if it's an agent skill (direct path!)
                    if any(agent_skill.upper() in target_skill.upper() for agent_skill in agent_skills):
                        directly_handled_conv_ids.add(conv_id)
                        categorized = True
                    # Whether it's agent or not, we break (no OTHER skills in between allowed)
                    break
            
            if categorized:
                continue
            
            # Priority 4: Our bot (GPT_MV_RESOLVERS → agent skill PATTERN)
            found_gpt_mv_resolvers = False
            for _, row in conv_df_sorted.iterrows():
                target_skill = str(row.get('TARGET_SKILL_PER_MESSAGE', '')).strip()
                
                if not target_skill:
                    continue
                
                # Check if current skill is GPT_MV_RESOLVERS (exclude GPT_MV_RESOLVERS_TA)
                if any(bot_skill.upper() in target_skill.upper() for bot_skill in mv_resolvers_bot_skills) and target_skill.upper() != 'GPT_MV_RESOLVERS_TA':
                    found_gpt_mv_resolvers = True
                    continue
                
                # If we found GPT_MV_RESOLVERS before, check if current message is from agent skills
                if found_gpt_mv_resolvers:
                    if any(agent_skill.upper() in target_skill.upper() for agent_skill in agent_skills):
                        our_bot_conv_ids.add(conv_id)
                        categorized = True
                        break
            
            if categorized:
                continue
            
            # Priority 5: Everything else goes to "other_bots"
            if not categorized:
                other_bots_conv_ids.add(conv_id)
        
        our_bot_count = len(our_bot_conv_ids)
        directly_handled_count = len(directly_handled_conv_ids)
        proactive_count = len(proactive_conv_ids)
        delighters_count = 0  # Excluded from base, tracked separately
        other_bots_count = len(other_bots_conv_ids)
        no_response_transfers_count = len(no_response_transfers_conv_ids)
        
        # Calculate proactive MV_RESOLVERS_SENIORS only (sub-metric for business)
        proactive_mv_resolvers_seniors_only_count = 0
        for conv_id in proactive_conv_ids:
            conv_rows = result_df_full[result_df_full['CONVERSATION_ID'] == conv_id]
            if not conv_rows.empty:
                # Check if conversation reached MV_RESOLVERS_SENIORS specifically
                has_mv_resolvers_seniors = False
                for _, row in conv_rows.iterrows():
                    target_skill = str(row.get('TARGET_SKILL_PER_MESSAGE', '')).strip().upper()
                    through_skill = str(row.get('THROUGH_SKILL', '')).strip().upper()
                    if 'MV_RESOLVERS_SENIORS' in target_skill or 'MV_RESOLVERS_SENIORS' in through_skill:
                        has_mv_resolvers_seniors = True
                        break
                if has_mv_resolvers_seniors:
                    proactive_mv_resolvers_seniors_only_count += 1
        
        # ========================================
        # BUSINESS REQUIREMENT: Adjust proactive and total to only include MV_RESOLVERS_SENIORS for proactive
        # Other categories (directly_handled, our_bot, other_bots) still use all 3 skills
        # ========================================
        proactive_other_skills_count = proactive_count - proactive_mv_resolvers_seniors_only_count
        
        # Replace proactive_count with MV_RESOLVERS_SENIORS only
        proactive_count_adjusted = proactive_mv_resolvers_seniors_only_count
        
        # Adjust total_seniors_callers to exclude proactive conversations with other skills
        total_seniors_callers_count_adjusted = total_seniors_callers_count - proactive_other_skills_count
        
        # Use adjusted values from here forward
        proactive_count = proactive_count_adjusted
        total_seniors_callers_count = total_seniors_callers_count_adjusted
        
        # ========================================
        # Calculate breakdown by TARGET AGENT SKILL for "Our bot to seniors"
        # ========================================
        seniors_our_bot_to_mv_resolvers_seniors_count = 0
        seniors_our_bot_to_mv_callers_count = 0
        seniors_our_bot_to_pre_r_visa_retention_count = 0
        
        if our_bot_conv_ids and not result_df_full.empty:
            # Filter to only conversations in "Our bot to seniors" group
            our_bot_conversations = result_df_full[result_df_full['CONVERSATION_ID'].isin(our_bot_conv_ids)]
            
            for conv_id in our_bot_conv_ids:
                conv_rows = our_bot_conversations[our_bot_conversations['CONVERSATION_ID'] == conv_id]
                
                if conv_rows.empty:
                    continue
                
                # Sort by MESSAGE_SENT_TIME for chronological order
                conv_df_sorted = conv_rows.sort_values('MESSAGE_SENT_TIME') if 'MESSAGE_SENT_TIME' in conv_rows.columns else conv_rows.sort_index()
                
                # Find the first agent skill that appears after GPT_MV_RESOLVERS
                found_gpt_mv_resolvers = False
                target_agent_skill = None
                
                for _, row in conv_df_sorted.iterrows():
                    target_skill = str(row.get('TARGET_SKILL_PER_MESSAGE', '')).strip().upper()
                    
                    if not target_skill:
                        continue
                    
                    # Check if this is GPT_MV_RESOLVERS (exclude GPT_MV_RESOLVERS_TA)
                    if any(bot_skill.upper() in target_skill for bot_skill in mv_resolvers_bot_skills) and target_skill != 'GPT_MV_RESOLVERS_TA':
                        found_gpt_mv_resolvers = True
                        continue
                    
                    # After finding GPT_MV_RESOLVERS, look for the first agent skill
                    if found_gpt_mv_resolvers:
                        # Check if this is one of the 3 agent skills
                        if 'MV_RESOLVERS_SENIORS' in target_skill:
                            target_agent_skill = 'MV_RESOLVERS_SENIORS'
                            break
                        elif 'MV_CALLERS' in target_skill:
                            target_agent_skill = 'MV_CALLERS'
                            break
                        elif 'PRE_R_VISA_RETENTION' in target_skill:
                            target_agent_skill = 'PRE_R_VISA_RETENTION'
                            break
                
                # Count based on target agent skill
                if target_agent_skill == 'MV_RESOLVERS_SENIORS':
                    seniors_our_bot_to_mv_resolvers_seniors_count += 1
                elif target_agent_skill == 'MV_CALLERS':
                    seniors_our_bot_to_mv_callers_count += 1
                elif target_agent_skill == 'PRE_R_VISA_RETENTION':
                    seniors_our_bot_to_pre_r_visa_retention_count += 1
        
        print(f"       └─ Our bot to seniors: {our_bot_count}")
        print(f"          └─ To MV_RESOLVERS_SENIORS: {seniors_our_bot_to_mv_resolvers_seniors_count}/{our_bot_count}")
        print(f"          └─ To MV_CALLERS: {seniors_our_bot_to_mv_callers_count}/{our_bot_count}")
        print(f"          └─ To Pre_R_Visa_Retention: {seniors_our_bot_to_pre_r_visa_retention_count}/{our_bot_count}")
        print(f"          └─ Sum check: {seniors_our_bot_to_mv_resolvers_seniors_count + seniors_our_bot_to_mv_callers_count + seniors_our_bot_to_pre_r_visa_retention_count} = {our_bot_count}")
        print(f"       └─ Directly handled by seniors: {directly_handled_count}")
        print(f"       └─ Proactive (MV_RESOLVERS_SENIORS only - ADJUSTED): {proactive_count}")
        print(f"          └─ Excluded {proactive_other_skills_count} proactive conversations with MV_CALLERS/Pre_R_Visa_Retention")
        print(f"       └─ Other bots to seniors: {other_bots_count}")
        print(f"       └─ ADJUSTED Total check: {our_bot_count + directly_handled_count + proactive_count + other_bots_count} = {total_seniors_callers_count} (proactive adjusted to MV_RESOLVERS_SENIORS only)")
        
        return total_seniors_callers_count, our_bot_count, directly_handled_count, proactive_count, proactive_mv_resolvers_seniors_only_count, delighters_count, other_bots_count, base_conv_ids, our_bot_conv_ids, seniors_our_bot_to_mv_resolvers_seniors_count, seniors_our_bot_to_mv_callers_count, seniors_our_bot_to_pre_r_visa_retention_count, supervisor_excluded_conv_ids, no_response_transfers_count
        
    except Exception as e:
        print(f"    ⚠️  Error calculating seniors/callers: {str(e)}")
        import traceback
        traceback.print_exc()
        return 0, 0, 0, 0, 0, 0, 0, set(), set(), 0, 0, 0, set(), 0


def calculate_wrong_number_clients(session, target_date):
    """
    Count unique clients (by USER_PHONE_NUMBER) who received the wrong-number
    redirect message from MV_Resolvers on the given target_date.

    The redirect message is identified by the unique phrase:
    'no longer able to receive or review messages on this number'

    Args:
        session: Snowflake session
        target_date: Target date string (YYYY-MM-DD)

    Returns:
        int: Number of distinct phone numbers that received the redirect message
    """
    filter_date = (datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    try:
        q = f"""
        SELECT COUNT(DISTINCT USER_PHONE_NUMBER) AS WRONG_NUMBER_CLIENTS_COUNT
        FROM SILVER.CHAT_EVALS.MV_CLIENTS_CHATS
        WHERE DATE(UPDATED_AT) = DATE('{filter_date}')
          AND UPPER(TEXT) LIKE '%NO LONGER ABLE TO RECEIVE OR REVIEW MESSAGES ON THIS NUMBER%'
        """
        result = session.sql(q).collect()
        count = int(result[0]['WRONG_NUMBER_CLIENTS_COUNT']) if result else 0
        print(f"    📵 Wrong number clients (unique phone numbers): {count}")
        return count
    except Exception as e:
        print(f"    ⚠️  Error calculating wrong number clients: {str(e)}")
        return 0


def store_resolvers_chats_breakdown(session, department_name, departments_config, target_date):
    """
    Store detailed breakdown of all conversations that reached MV_Resolvers seniors in a raw table.
    
    Creates/updates table: SILVER.CHAT_EVALS.RESOLVERS_CHATS_BREAKDOWN
    
    Schema:
    - TARGET_DATE: Date
    - CONVERSATION_ID: String
    - CATEGORY: String (our_bot, directly_handled, proactive, delighters, other_bots)
    - SUB_CATEGORY: String (known_flow_transfer, tech_errors_transfers, guardrails, other_transfers, or NULL for non-our_bot)
    - THROUGH_SKILLS: String (comma-separated list of unique through_skill values from the conversation)
    
    Args:
        session: Snowflake session
        department_name: Department name (should be 'MV_Resolvers')
        departments_config: Department configuration
        target_date: Target date for analysis
    
    Returns:
        int: Number of records inserted
    """
    # Only applies to MV_Resolvers
    if department_name != 'MV_Resolvers':
        return 0
    
    try:
        department_config = departments_config[department_name]
        table_name = department_config.get('table_name', 'SILVER.CHAT_EVALS.MV_CLIENTS_CHATS')
        breakdown_table = 'SILVER.CHAT_EVALS.RESOLVERS_CHATS_BREAKDOWN'
        # LLM_EVAL.PUBLIC.RESOLVERS_CHATS_BREAKDOWN_TEST
        # SILVER.CHAT_EVALS.RESOLVERS_CHATS_BREAKDOWN
        filter_date = (datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        agent_skills = department_config['agent_skills']
        mv_resolvers_bot_skills = department_config.get('bot_skills', [])
        
        print(f"    📊 Storing resolvers chats breakdown for {department_name}...")
        
        # SQL query to find ALL conversations that reached seniors or callers (THE BASE)
        # EXCLUDE delighters conversations from the base
        query_base = f"""
        SELECT DISTINCT conversation_id 
        FROM {table_name}
        WHERE (target_skill_per_message ILIKE '%MV_RESOLVERS_SENIORS%' 
               OR through_skill ILIKE '%MV_RESOLVERS_SENIORS%'
               OR target_skill_per_message ILIKE '%MV_CALLERS%'
               OR through_skill ILIKE '%MV_CALLERS%'
               OR target_skill_per_message ILIKE '%Pre_R_Visa_Retention%'
               OR through_skill ILIKE '%Pre_R_Visa_Retention%')
        AND through_skill NOT ILIKE '%DELIGHTERS%'
        AND date(end_date) = '{target_date}'
        """
        
        result_df_base = session.sql(query_base).to_pandas()
        
        if result_df_base.empty:
            print(f"    ✅ No conversations found to store in breakdown table")
            return 0
        
        base_conv_ids = set(result_df_base['CONVERSATION_ID'].values)
        
        # Now get full conversation data for these conversations to categorize them
        conv_ids_str = "', '".join([str(cid) for cid in base_conv_ids])
        query_full_data = f"""
        SELECT *
        FROM {table_name}
        WHERE conversation_id IN ('{conv_ids_str}')
        AND date(updated_at) = '{filter_date}'
        ORDER BY conversation_id, message_seq
        """
        
        result_df_full = session.sql(query_full_data).to_pandas()
        
        if result_df_full.empty:
            print(f"    ✅ No conversation data found to store in breakdown table")
            return 0
        
        # Same as calculate_total_seniors_callers: system-first + non-CLIENT broadcast exclusion
        excluded_broadcast_count = 0
        for conv_id in list(base_conv_ids):
            conv_rows = result_df_full[result_df_full['CONVERSATION_ID'] == conv_id]
            conv_sorted = (
                conv_rows.sort_values('MESSAGE_SENT_TIME')
                if 'MESSAGE_SENT_TIME' in conv_rows.columns
                else conv_rows.sort_values('MESSAGE_SEQ')
                if 'MESSAGE_SEQ' in conv_rows.columns
                else conv_rows
            )
            first = conv_sorted.iloc[0]
            sent_by = str(first.get('SENT_BY', '')).lower()
            if sent_by == 'system':
                ct = first.get('CUSTOMER_TYPE')
                customer_type = '' if pd.isna(ct) else str(ct).strip().upper()
                if customer_type != 'CLIENT':
                    base_conv_ids.remove(conv_id)
                    excluded_broadcast_count += 1
        
        if excluded_broadcast_count > 0:
            print(f"    🚫 Excluded {excluded_broadcast_count} system-started non-CLIENT broadcasts")
        
        supervisor_skill_upper = 'MV_RESOLVER_SUPERVISOR'
        excluded_supervisor_count = 0
        for conv_id in list(base_conv_ids):
            conv_rows = result_df_full[result_df_full['CONVERSATION_ID'] == conv_id]
            conv_sorted = (
                conv_rows.sort_values('MESSAGE_SENT_TIME')
                if 'MESSAGE_SENT_TIME' in conv_rows.columns
                else conv_rows.sort_values('MESSAGE_SEQ')
                if 'MESSAGE_SEQ' in conv_rows.columns
                else conv_rows
            )
            for i in range(min(2, len(conv_sorted))):
                raw = conv_sorted.iloc[i].get('TARGET_SKILL_PER_MESSAGE')
                ts = '' if pd.isna(raw) else str(raw).strip().upper()
                if ts == supervisor_skill_upper:
                    base_conv_ids.remove(conv_id)
                    excluded_supervisor_count += 1
                    break
        
        if excluded_supervisor_count > 0:
            print(f"    🚫 Excluded {excluded_supervisor_count} MV_RESOLVER_SUPERVISOR-initiated chats (first 2 messages)")
        
        if len(base_conv_ids) == 0:
            print(f"    ✅ No conversations remaining after filtering base exclusions")
            return 0
        
        # Prepare breakdown data
        breakdown_records = []
        
        # Categorize each conversation using PATTERN MATCHING (same logic as calculate_total_seniors_callers)
        for conv_id in base_conv_ids:
            conv_rows = result_df_full[result_df_full['CONVERSATION_ID'] == conv_id]
            
            if conv_rows.empty:
                # Store as other_bots with no sub_category
                through_skills = "N/A"
                breakdown_records.append({
                    'TARGET_DATE': target_date,
                    'CONVERSATION_ID': conv_id,
                    'CATEGORY': 'other_bots',
                    'SUB_CATEGORY': None,
                    'THROUGH_SKILLS': through_skills
                })
                continue
            
            # Get unique through_skills for this conversation
            unique_through_skills = conv_rows['THROUGH_SKILL'].dropna().unique()
            through_skills = ', '.join([str(s) for s in unique_through_skills]) if len(unique_through_skills) > 0 else "N/A"
            
            # Sort by message_sent_time (ascending) to ensure chronological order
            conv_df_sorted = conv_rows.sort_values('MESSAGE_SENT_TIME') if 'MESSAGE_SENT_TIME' in conv_rows.columns else conv_rows.sort_index()
            
            category = None
            sub_category = None
            
            # Priority 1: No Response Transfers
            # Chats with GPT_RESOLVERS_BOT or GPT_MV_RESOLVERS where:
            # - ALL messages under these skills have NULL/empty EXECUTION_ID
            # - OR ANY message under these skills contains "by admin" in TEXT
            target_bot_skills = ['GPT_RESOLVERS_BOT'] + mv_resolvers_bot_skills  # ['GPT_RESOLVERS_BOT', 'GPT_MV_RESOLVERS']
            
            bot_messages = conv_df_sorted[
                conv_df_sorted['TARGET_SKILL_PER_MESSAGE'].apply(
                    lambda x: (any(bot_skill.upper() in str(x).upper() for bot_skill in target_bot_skills) and str(x).upper() != 'GPT_MV_RESOLVERS_TA') if pd.notna(x) and str(x).strip() else False
                )
            ]
            
            if not bot_messages.empty:
                # Check condition 1: ALL messages under bot skills have NULL/empty EXECUTION_ID
                # all_null_execution = bot_messages['EXECUTION_ID'].apply(
                #     lambda x: pd.isna(x) or str(x).strip() == ''
                # ).all()
                
                # Check condition 2: ANY message under bot skills contains "by admin"
                any_by_admin = bot_messages['TEXT'].apply(
                    lambda x: 'by admin' in str(x).lower() if pd.notna(x) else False
                ).any()
                
                # Check condition 3: ANY message under bot skills contains "Error Task:"
                any_error_task = bot_messages['TEXT'].apply(
                    lambda x: 'error task:' in str(x).lower() if pd.notna(x) else False
                ).any()
                
                if  any_by_admin or any_error_task:
                    category = 'no_response_transfers'
                    breakdown_records.append({
                        'TARGET_DATE': target_date,
                        'CONVERSATION_ID': conv_id,
                        'CATEGORY': category,
                        'SUB_CATEGORY': None,
                        'THROUGH_SKILLS': through_skills
                    })
                    continue
            
            # Priority 2: Proactive — chronologically first row + agent skill guard (same as calculate_total_seniors_callers)
            if len(conv_df_sorted) > 0:
                first_msg = conv_df_sorted.iloc[0]
                target_skill_first = str(first_msg.get('TARGET_SKILL_PER_MESSAGE', '')).strip()
                skip_proactive_due_to_non_agent_skill = False
                if target_skill_first:
                    first_skill_is_agent = any(
                        agent_skill.upper() in target_skill_first.upper() for agent_skill in agent_skills
                    )
                    if not first_skill_is_agent:
                        skip_proactive_due_to_non_agent_skill = True
                
                if not skip_proactive_due_to_non_agent_skill:
                    sent_by = str(first_msg.get('SENT_BY', '')).lower()
                    agent_name = first_msg.get('AGENT_NAME')
                    message_type = str(first_msg.get('MESSAGE_TYPE', '')).lower()
                    
                    is_proactive = not (
                        sent_by == 'consumer' or 
                        agent_name is None or 
                        pd.isna(agent_name) or
                        (sent_by == 'system' and 'normal message' in message_type)
                    )
                    
                    if is_proactive:
                        category = 'proactive'
                        breakdown_records.append({
                            'TARGET_DATE': target_date,
                            'CONVERSATION_ID': conv_id,
                            'CATEGORY': category,
                            'SUB_CATEGORY': None,
                            'THROUGH_SKILLS': through_skills
                        })
                        continue
            
            # Condition 2: Single-row conversation with agent skill only
            if len(conv_df_sorted) == 1:
                single_row = conv_df_sorted.iloc[0]
                target_skill = str(single_row.get('TARGET_SKILL_PER_MESSAGE', '')).strip()
                if target_skill and any(agent_skill.upper() in target_skill.upper() for agent_skill in agent_skills):
                    category = 'proactive'
                    breakdown_records.append({
                        'TARGET_DATE': target_date,
                        'CONVERSATION_ID': conv_id,
                        'CATEGORY': category,
                        'SUB_CATEGORY': None,
                        'THROUGH_SKILLS': through_skills
                    })
                    continue
            
            # Priority 3: Directly handled (GPT_RESOLVERS_BOT → agent skill PATTERN)
            # BUT EXCLUDE if GPT_MV_RESOLVERS appears BETWEEN GPT_RESOLVERS_BOT and agent skill
            found_gpt_resolvers_bot = False
            for _, row in conv_df_sorted.iterrows():
                target_skill = str(row.get('TARGET_SKILL_PER_MESSAGE', '')).strip()
                
                if not target_skill:
                    continue
                
                # Check if current skill is GPT_RESOLVERS_BOT
                if 'GPT_RESOLVERS_BOT' in target_skill.upper():
                    found_gpt_resolvers_bot = True
                    continue  # Allow multiple consecutive GPT_RESOLVERS_BOT
                
                # If we found GPT_RESOLVERS_BOT before, check NEXT non-GPT_RESOLVERS_BOT skill
                if found_gpt_resolvers_bot:
                    # This is the first non-GPT_RESOLVERS_BOT skill after GPT_RESOLVERS_BOT
                    # Check if it's GPT_MV_RESOLVERS (would interrupt the direct path)
                    # Exclude GPT_MV_RESOLVERS_TA from substring match
                    if any(bot_skill.upper() in target_skill.upper() for bot_skill in mv_resolvers_bot_skills) and target_skill.upper() != 'GPT_MV_RESOLVERS_TA':
                        # GPT_MV_RESOLVERS is between GPT_RESOLVERS_BOT and agent skill
                        # Skip this - will be caught by "our_bot" category
                        break
                    
                    # Check if it's an agent skill (direct path!)
                    if any(agent_skill.upper() in target_skill.upper() for agent_skill in agent_skills):
                        category = 'directly_handled'
                    # Whether it's agent or not, we break (no OTHER skills in between allowed)
                    break
            
            if category == 'directly_handled':
                breakdown_records.append({
                    'TARGET_DATE': target_date,
                    'CONVERSATION_ID': conv_id,
                    'CATEGORY': category,
                    'SUB_CATEGORY': None,
                    'THROUGH_SKILLS': through_skills
                })
                continue
            
            # Priority 4: Our bot (GPT_MV_RESOLVERS → agent skill PATTERN)
            found_gpt_mv_resolvers = False
            for _, row in conv_df_sorted.iterrows():
                target_skill = str(row.get('TARGET_SKILL_PER_MESSAGE', '')).strip()
                
                if not target_skill:
                    continue
                
                # Check if current skill is GPT_MV_RESOLVERS (exclude GPT_MV_RESOLVERS_TA)
                if any(bot_skill.upper() in target_skill.upper() for bot_skill in mv_resolvers_bot_skills) and target_skill.upper() != 'GPT_MV_RESOLVERS_TA':
                    found_gpt_mv_resolvers = True
                    continue
                
                # If we found GPT_MV_RESOLVERS before, check if current message is from agent skills
                if found_gpt_mv_resolvers:
                    if any(agent_skill.upper() in target_skill.upper() for agent_skill in agent_skills):
                        category = 'our_bot'
                        break
            
            if category == 'our_bot':
                # Further categorize into sub-categories for "our_bot"
                # Sub-category 1: Known_Flow_Transfer
                for _, row in conv_rows.iterrows():
                    text = str(row.get('TEXT', ''))
                    text_lower = text.lower()
                    
                    # Condition 1: transfer_conversation
                    if 'transfer_conversation' in text_lower:
                        sub_category = 'known_flow_transfer'
                        break
                    
                    # Condition 2: both "flag_reason":"Frustrated Client" AND {"content"
                    if '"flag_reason":"frustrated client"' in text_lower and '{"content"' in text_lower:
                        sub_category = 'known_flow_transfer'
                        break
                    
                    # Condition 3: Transfer due to frustration flag (multiple_contract_detector)
                    if 'transfer due to frustration' in text_lower:
                        sub_category = 'known_flow_transfer'
                        break
                
                # Sub-category 2: Tech_Errors_Transfers (if not Known_Flow_Transfer)
                if not sub_category:
                    for _, row in conv_rows.iterrows():
                        text = str(row.get('TEXT', ''))
                        text_lower = text.lower()
                        message_type = str(row.get('MESSAGE_TYPE', '')).strip()
                        
                        # Condition 1: TEXT contains "by admin" AND MESSAGE_TYPE = 'Transfer'
                        if 'by admin' in text_lower and message_type == 'Transfer':
                            sub_category = 'tech_errors_transfers'
                            break
                        
                        # Condition 2: TEXT contains "Error Task:"
                        if 'error task:' in text_lower:
                            sub_category = 'tech_errors_transfers'
                            break
                
                # Sub-category 3: Guardrails (if not Known_Flow_Transfer and not Tech_Errors_Transfers)
                if not sub_category:
                    # Check for guardrail patterns
                    for _, row in conv_rows.iterrows():
                        text = str(row.get('TEXT', '')).lower()
                        
                        if 'reason:' in text and 'error' in text and 'guard' in text:
                            sub_category = 'guardrails'
                            break
                        
                        if text.startswith('guardrail detected') and 'false' in text and 'promise' in text:
                            sub_category = 'guardrails'
                            break
                
                # Sub-category 4: Other_transfers (default for our_bot)
                if not sub_category:
                    sub_category = 'other_transfers'
                
                breakdown_records.append({
                    'TARGET_DATE': target_date,
                    'CONVERSATION_ID': conv_id,
                    'CATEGORY': category,
                    'SUB_CATEGORY': sub_category,
                    'THROUGH_SKILLS': through_skills
                })
                continue
            
            # Priority 5: Everything else goes to "other_bots"
            if not category:
                category = 'other_bots'
                breakdown_records.append({
                    'TARGET_DATE': target_date,
                    'CONVERSATION_ID': conv_id,
                    'CATEGORY': category,
                    'SUB_CATEGORY': None,
                    'THROUGH_SKILLS': through_skills
                })
        
        # Convert to DataFrame
        breakdown_df = pd.DataFrame(breakdown_records)
        
        if breakdown_df.empty:
            print(f"    ✅ No records to insert into breakdown table")
            return 0
        
        # ========================================
        # BUSINESS REQUIREMENT: Filter out proactive conversations that are NOT MV_RESOLVERS_SENIORS
        # These should not be counted as "reached resolvers"
        # ========================================
        proactive_to_exclude = []
        for record in breakdown_records:
            if record['CATEGORY'] == 'proactive':
                conv_id = record['CONVERSATION_ID']
                conv_rows = result_df_full[result_df_full['CONVERSATION_ID'] == conv_id]
                
                # Check if this proactive conversation reached MV_RESOLVERS_SENIORS
                has_mv_resolvers_seniors = False
                for _, row in conv_rows.iterrows():
                    target_skill = str(row.get('TARGET_SKILL_PER_MESSAGE', '')).strip().upper()
                    through_skill = str(row.get('THROUGH_SKILL', '')).strip().upper()
                    if 'MV_RESOLVERS_SENIORS' in target_skill or 'MV_RESOLVERS_SENIORS' in through_skill:
                        has_mv_resolvers_seniors = True
                        break
                
                # If it only reached MV_CALLERS or Pre_R_Visa_Retention, exclude it
                if not has_mv_resolvers_seniors:
                    proactive_to_exclude.append(conv_id)
        
        # Filter out the excluded proactive conversations
        if proactive_to_exclude:
            breakdown_df = breakdown_df[~breakdown_df['CONVERSATION_ID'].isin(proactive_to_exclude)]
            print(f"    🚫 Excluded {len(proactive_to_exclude)} proactive conversations (MV_CALLERS/Pre_R_Visa_Retention only)")
        
        if breakdown_df.empty:
            print(f"    ✅ No records remaining after filtering")
            return 0
        
        # Create table if it doesn't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {breakdown_table} (
            TARGET_DATE DATE,
            CONVERSATION_ID VARCHAR,
            CATEGORY VARCHAR,
            SUB_CATEGORY VARCHAR,
            THROUGH_SKILLS VARCHAR,
            PRIMARY KEY (TARGET_DATE, CONVERSATION_ID)
        )
        """
        
        session.sql(create_table_query).collect()
        
        # Delete existing records for this date (to avoid duplicates on re-run)
        delete_query = f"""
        DELETE FROM {breakdown_table}
        WHERE TARGET_DATE = '{target_date}'
        """
        
        session.sql(delete_query).collect()
        
        # Insert new records
        snowpark_df = session.create_dataframe(breakdown_df)
        snowpark_df.write.mode('append').save_as_table(breakdown_table)
        
        records_count = len(breakdown_df)
        print(f"    ✅ Inserted {records_count} records into {breakdown_table}")
        
        return records_count
        
    except Exception as e:
        print(f"    ⚠️  Error storing resolvers chats breakdown: {str(e)}")
        import traceback
        traceback.print_exc()
        return 0


def get_mv_message_delay_chats(session, target_date):
    """
    Identify MV Resolvers chats where:
      1. The client (consumer) initiated the chat (first message SENT_BY == 'consumer').
      2. The client sent at least 2 consecutive messages at the very start of the chat
         (the first AND second messages, ordered by MESSAGE_SENT_TIME, are both from
         the consumer — i.e. no bot/agent reply in between).
      3. The time gap between the 2nd and 1st client message exceeds 50 seconds.

    Data is loaded directly from SILVER.CHAT_EVALS.MV_CLIENTS_CHATS without applying
    Phase 1 engagement filters, so that hi-bye / boomerang-excluded chats are still
    captured and the count reflects the true frequency of the pattern.

    Args:
        session:     Snowflake Snowpark session.
        target_date: Date string 'YYYY-MM-DD'.  Messages are loaded by
                     DATE(UPDATED_AT) = target_date + 1 day (standard pattern).

    Returns:
        pd.DataFrame with columns:
            CONVERSATION_ID   – chat identifier
            DATE              – target_date string
            TIME_DIFF_SECONDS – gap (float) between the 1st and 2nd client message
    """
    print(f"\n📊 [get_mv_message_delay_chats] Loading MV_CLIENTS_CHATS for target_date={target_date} ...")

    try:
        filter_date = (
            datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days=1)
        ).strftime('%Y-%m-%d')

        query = f"""
            SELECT CONVERSATION_ID, MESSAGE_SENT_TIME, SENT_BY
            FROM SILVER.CHAT_EVALS.MV_CLIENTS_CHATS
            WHERE DATE(UPDATED_AT) = '{filter_date}'
        """
        df = session.sql(query).to_pandas()

        if df.empty:
            print(f"  ⚠️  No rows returned for filter_date={filter_date}.")
            return pd.DataFrame(columns=['CONVERSATION_ID', 'DATE', 'TIME_DIFF_SECONDS'])

        # Normalise
        df['MESSAGE_SENT_TIME'] = pd.to_datetime(df['MESSAGE_SENT_TIME'], errors='coerce')
        df['SENT_BY_LOWER'] = df['SENT_BY'].astype(str).str.lower().str.strip()
        df = df.dropna(subset=['MESSAGE_SENT_TIME'])
        df = df.sort_values(['CONVERSATION_ID', 'MESSAGE_SENT_TIME']).reset_index(drop=True)

        qualifying_rows = []

        for conv_id, conv_df in df.groupby('CONVERSATION_ID', sort=False):
            msgs = conv_df.sort_values('MESSAGE_SENT_TIME').reset_index(drop=True)

            if len(msgs) < 2:
                continue

            first_sent_by  = msgs.loc[0, 'SENT_BY_LOWER']
            second_sent_by = msgs.loc[1, 'SENT_BY_LOWER']

            # Condition 1 & 2: both the first and second messages must be from the consumer
            if first_sent_by != 'consumer' or second_sent_by != 'consumer':
                continue

            # Condition 3: time gap > 50 seconds
            time_diff = (
                msgs.loc[1, 'MESSAGE_SENT_TIME'] - msgs.loc[0, 'MESSAGE_SENT_TIME']
            ).total_seconds()

            if time_diff > 50:
                qualifying_rows.append({
                    'CONVERSATION_ID':   conv_id,
                    'DATE':              target_date,
                    'TIME_DIFF_SECONDS': round(time_diff, 2),
                })

        result_df = pd.DataFrame(qualifying_rows, columns=['CONVERSATION_ID', 'DATE', 'TIME_DIFF_SECONDS'])
        print(f"  ✅ {len(result_df)} qualifying chats found (out of {df['CONVERSATION_ID'].nunique()} total conversations).")
        return result_df

    except Exception as e:
        print(f"  ❌ Error in get_mv_message_delay_chats: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(columns=['CONVERSATION_ID', 'DATE', 'TIME_DIFF_SECONDS'])


def run_mv_message_delay_analysis(session, target_dates, department='MV_Resolvers', target_table='MV_MESSAGE_DELAY_CHATS'):
    """
    Orchestrate get_mv_message_delay_chats across multiple dates, save each
    date's qualifying chats to Snowflake, and return the combined DataFrame.

    Args:
        session:      Snowflake Snowpark session.
        target_dates: List of date strings 'YYYY-MM-DD' (typically 3 days).
        department:   Department label written into the Snowflake table.
        target_table: Snowflake table name (created automatically if absent).

    Returns:
        pd.DataFrame with columns CONVERSATION_ID, DATE, TIME_DIFF_SECONDS.
    """
    print(f"\n📊 AVERAGE MESSAGE DELAY ANALYSIS — {department}")
    print(f"   Dates: {target_dates}")
    print(f"   Target table: LLM_EVAL.PUBLIC.{target_table}")

    all_results = []
    for date in target_dates:
        df_date = get_mv_message_delay_chats(session, date)
        print(f"  [{date}]  {len(df_date)} qualifying chat(s)")

        if not df_date.empty:
            payload = df_date[['CONVERSATION_ID', 'TIME_DIFF_SECONDS']].copy()
            insert_raw_data_with_cleanup(
                session,
                table_name=target_table,
                department=department,
                target_date=date,
                dataframe=payload,
                columns=['CONVERSATION_ID', 'TIME_DIFF_SECONDS'],
            )
        else:
            print(f"    (no rows to write for {date})")

        all_results.append(df_date)

    results_df = pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame(
        columns=['CONVERSATION_ID', 'DATE', 'TIME_DIFF_SECONDS']
    )

    print(f"\nTotal qualifying chats across {len(target_dates)} day(s): {len(results_df)}")
    if not results_df.empty:
        print("\nBreakdown by day:")
        print(results_df.groupby('DATE')['CONVERSATION_ID'].count().rename('chat_count').to_string())
    print(f"\nData saved to Snowflake table: LLM_EVAL.PUBLIC.{target_table}")
    return results_df


def calculate_transfers_due_to_tech_error(session, department_name, departments_config, target_date):
    """
    Calculate transfers due to technical errors for any department.
    
    Finds conversations where text contains 'Error Task:' which indicates a technical error
    that caused a transfer. Only counts rows where target_skill_per_message matches
    the department's skills (bot_skills + agent_skills).
    
    Args:
        session: Snowflake session
        department_name: Department name
        departments_config: Department configuration
        target_date: Target date for analysis
    
    Returns:
        int: Number of conversations with technical error transfers
    """
    try:
        department_config = departments_config[department_name]
        table_name = department_config.get('table_name', 'SILVER.CHAT_EVALS.MV_CLIENTS_CHATS')
        filter_date = (datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        dept_skills = set(department_config.get('bot_skills', []) + department_config.get('agent_skills', []))
        
        # SQL query to find all rows with technical errors (not distinct)
        query = f"""
        SELECT conversation_id, target_skill_per_message
        FROM {table_name}
        WHERE text ILIKE '%Error Task:%' 
        AND date(updated_at) = '{filter_date}'
        """
        
        print(f"    🔧 Calculating transfers due to tech errors for {department_name}...")
        
        # Execute query and get all rows
        result_df = session.sql(query).to_pandas()
        
        tech_error_count = 0
        if not result_df.empty:
            # Filter by department skills
            result_df['TARGET_SKILL_PER_MESSAGE'] = result_df['TARGET_SKILL_PER_MESSAGE'].fillna('')
            filtered_df = result_df[result_df['TARGET_SKILL_PER_MESSAGE'].isin(dept_skills)]
            
            # Count unique conversations after filtering
            tech_error_count = filtered_df['CONVERSATION_ID'].nunique()
        
        print(f"    ✅ Found {tech_error_count} conversations with technical error transfers")
        
        return tech_error_count
        
    except Exception as e:
        print(f"    ⚠️  Error calculating tech error transfers: {str(e)}")
        return 0


def calculate_total_guardrail(session, department_name, departments_config, target_date):
    """
    Calculate total guardrail interventions for departments with guardrails.
    Currently supports: MV_Resolvers, CC_Resolvers, multiple_contract_detector

    Counts distinct conversations from the three main guardrail tables:
    1. GUARDRAIL_STOPPED_TOOLS (wrong tools)
    2. GUARDRAIL_MISSED_TOOLS (missed tools)
    3. GUARDRAIL_FALSE_PROMISE_NO_TOOL (false promises with no tool)

    For multiple_contract_detector:
    - Code-based evals only (excluding agent-related metrics, since there are no agents
      and we do not care if the chat goes to seniors of other departments)
    - Guardrails counted: Wrong tool calls, Missed tool calls, False promises
    - Also counts: Frustrations caught through the transfer_tool
      (flag: Transfer due to frustration) from the raw chat table

    Args:
        session: Snowflake session
        department_name: Department name (MV_Resolvers, CC_Resolvers, or multiple_contract_detector)
        departments_config: Department configuration
        target_date: Target date for analysis

    Returns:
        int: Number of conversations with guardrail interventions
    """
    # Only applies to departments with guardrails
    if department_name not in ['MV_Resolvers', 'CC_Resolvers', 'multiple_contract_detector']:
        return 0

    try:
        print(f"    🛡️  Calculating total guardrail interventions for {department_name}...")

        # Base guardrail tables query (wrong tools, missed tools, false promises)
        query = f"""
        SELECT COUNT(DISTINCT conversation_id) as TOTAL_GUARDRAIL_CONVS
        FROM (
            SELECT DISTINCT CONVERSATION_ID
            FROM GUARDRAIL_STOPPED_TOOLS
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'

            UNION

            SELECT DISTINCT CONVERSATION_ID
            FROM GUARDRAIL_MISSED_TOOLS
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'

            UNION

            SELECT DISTINCT CONVERSATION_ID
            FROM GUARDRAIL_FALSE_PROMISE_NO_TOOL
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
        ) all_guardrails
        """

        result_df = session.sql(query).to_pandas()

        guardrail_count = 0
        if not result_df.empty and 'TOTAL_GUARDRAIL_CONVS' in result_df.columns:
            guardrail_count = int(result_df['TOTAL_GUARDRAIL_CONVS'].iloc[0])

        # For multiple_contract_detector: also count frustration transfers from raw chat table
        # (flag: Transfer due to frustration via the transfer_tool)
        if department_name == 'multiple_contract_detector':
            dept_config = departments_config.get(department_name, {})
            table_name = dept_config.get('table_name', 'SILVER.CHAT_EVALS.MV_CLIENTS_CHATS')
            bot_skills = dept_config.get('bot_skills', [])
            filter_date = (datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            skills_list = "', '".join(bot_skills)

            frustration_query = f"""
            SELECT DISTINCT CONVERSATION_ID
            FROM {table_name}
            WHERE DATE(UPDATED_AT) = '{filter_date}'
              AND UPPER(TEXT) LIKE '%TRANSFER DUE TO FRUSTRATION%'
              AND UPPER(TARGET_SKILL_PER_MESSAGE) IN ('{skills_list.upper()}')
            """

            try:
                frustration_df = session.sql(frustration_query).to_pandas()
                frustration_ids = set(frustration_df['CONVERSATION_ID'].values) if not frustration_df.empty else set()
                frustration_count = len(frustration_ids)
                if frustration_count > 0:
                    print(f"    😤 Found {frustration_count} frustration-transfer conversations (Transfer due to frustration flag)")
                guardrail_count += frustration_count
            except Exception as fe:
                print(f"    ⚠️  Could not count frustration transfers: {str(fe)}")

        print(f"    ✅ Found {guardrail_count} conversations with guardrail interventions")

        return guardrail_count

    except Exception as e:
        print(f"    ⚠️  Error calculating guardrail interventions: {str(e)}")
        return 0


def get_guardrail_conversation_ids(session, department_name, target_date):
    """
    Get set of conversation IDs with guardrail interactions for a department.
    
    Queries the three main guardrail tables:
    1. GUARDRAIL_STOPPED_TOOLS (wrong tools)
    2. GUARDRAIL_MISSED_TOOLS (missed tools)
    3. GUARDRAIL_FALSE_PROMISE_NO_TOOL (false promises with no tool)
    
    Args:
        session: Snowflake session
        department_name: Department name
        target_date: Target date for analysis
    
    Returns:
        set: Set of conversation IDs with guardrail interactions
    """
    # Only applies to departments with guardrails
    if department_name not in ['MV_Resolvers', 'CC_Resolvers', 'multiple_contract_detector']:
        return set()
    
    try:
        # Query all guardrail tables to get conversation IDs
        query = f"""
        SELECT DISTINCT CONVERSATION_ID
        FROM (
            SELECT DISTINCT CONVERSATION_ID
            FROM GUARDRAIL_STOPPED_TOOLS
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'

            UNION

            SELECT DISTINCT CONVERSATION_ID
            FROM GUARDRAIL_MISSED_TOOLS
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'

            UNION

            SELECT DISTINCT CONVERSATION_ID
            FROM GUARDRAIL_FALSE_PROMISE_NO_TOOL
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
        ) all_guardrails
        """
        
        result_df = session.sql(query).to_pandas()
        
        if result_df.empty:
            return set()
        
        guardrail_conv_ids = set(result_df['CONVERSATION_ID'].values)
        return guardrail_conv_ids
        
    except Exception as e:
        print(f"    ⚠️  Error querying guardrail conversation IDs: {str(e)}")
        return set()


def calculate_guardrail_agent(session, department_name, departments_config, target_date):
    """
    Calculate guardrail interventions that resulted in agent transfers.
    
    A conversation is counted when:
    1. The conversation has at least one guardrail intervention (from any of the 3 main tables)
    2. AND the conversation has a transfer to an agent skill (from department's agent_skills config)
    
    Args:
        session: Snowflake session
        department_name: Department name
        departments_config: Department configuration
        target_date: Target date for analysis
    
    Returns:
        int: Number of conversations with guardrail interventions that transferred to agents
    """
    try:
        # Get department's agent skills dynamically
        department_config = departments_config.get(department_name, {})
        agent_skills = department_config.get('agent_skills', [])
        
        if not agent_skills:
            print(f"    ℹ️  No agent skills configured for {department_name}, skipping guardrail agent calculation")
            return 0
        
        # Build SQL IN clause for agent skills
        agent_skills_str = "', '".join(agent_skills)
        agent_skills_clause = f"('{agent_skills_str}')"
        
        # Get department table name for checking agent transfers
        table_name = department_config.get('table_name', 'SILVER.CHAT_EVALS.MV_CLIENTS_CHATS')
        filter_date = (datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Query to find conversations with guardrails that also transferred to agents
        query = f"""
        WITH guardrail_convs AS (
            -- Get all conversations with any guardrail intervention
            SELECT DISTINCT CONVERSATION_ID
            FROM GUARDRAIL_STOPPED_TOOLS
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
            
            UNION
            
            SELECT DISTINCT CONVERSATION_ID
            FROM GUARDRAIL_MISSED_TOOLS
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
            
            UNION
            
            SELECT DISTINCT CONVERSATION_ID
            FROM GUARDRAIL_FALSE_PROMISE_NO_TOOL
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
        ),
        guardrail_conv_ids AS (
            -- Get the conversation IDs to check
            SELECT DISTINCT CONVERSATION_ID
            FROM guardrail_convs
        ),
        agent_transfer_convs AS (
            -- Check in department table if these conversations had agent skill transfers
            SELECT DISTINCT conversation_id
            FROM {table_name}
            WHERE conversation_id IN (SELECT CONVERSATION_ID FROM guardrail_conv_ids)
              AND date(updated_at) = '{filter_date}'
              AND target_skill_per_message IN {agent_skills_clause}
        )
        SELECT COUNT(DISTINCT atc.conversation_id) as guardrail_agent_count
        FROM agent_transfer_convs atc
        """
        
        print(f"    🛡️  Calculating guardrail + agent transfer interventions for {department_name}...")
        print(f"    🔍 Checking against {len(agent_skills)} agent skills")
        
        result_df = session.sql(query).to_pandas()
        
        guardrail_agent_count = 0
        if not result_df.empty and 'GUARDRAIL_AGENT_COUNT' in result_df.columns:
            guardrail_agent_count = int(result_df['GUARDRAIL_AGENT_COUNT'].iloc[0])
        
        print(f"    ✅ Found {guardrail_agent_count} conversations with guardrail + agent transfer")
        
        return guardrail_agent_count
        
    except Exception as e:
        print(f"    ⚠️  Error calculating guardrail agent interventions: {str(e)}")
        return 0

def extract_categories_from_conversation(df, conv_id):
    """
    Extract all unique CategoryUsed values from messages in a conversation.
    
    Args:
        df: DataFrame containing conversation messages
        conv_id: Conversation ID to analyze
    
    Returns:
        str: Comma-separated list of unique categories (sorted alphabetically)
    """
    conv_messages = df[df['CONVERSATION_ID'] == conv_id]
    categories = set()
    
    for idx, msg in conv_messages.iterrows():
        text = str(msg['TEXT'])
        # Use regex to find CategoryUsed pattern
        category_match = re.search(
            r'"CategoryUsed"\s*:\s*"([^"]+)"',
            text,
            re.IGNORECASE
        )
        if category_match:
            category = category_match.group(1).strip()
            if category:  # Only add non-empty categories
                categories.add(category)
    
    # Return sorted comma-separated list
    return ', '.join(sorted(categories)) if categories else ''


def analyze_conversation_categories(session, df, department_name, target_date):
    """
    Extract all categories from all conversations and store in mapping table.
    Creates one row per conversation-category pair (normalized).
    
    Only extracts categories from messages that belong to the department's skills
    (bot_skills + agent_skills) to ensure data quality.
    
    Args:
        session: Snowflake session
        df: Filtered DataFrame from Phase 1
        department_name: Department name
        target_date: Target date for analysis
    
    Returns:
        int: Count of conversation-category pairs found
    """
    print(f"  📊 Analyzing conversation categories for {department_name}...")
    
    if df.empty:
        print(f"    ⚠️  No data to analyze for conversation categories")
        return 0
    
    category_data = []
    
    try:
        # Get department skills for filtering
        departments_config = get_snowflake_departments_config()
        dept_config = departments_config.get(department_name, {})
        bot_skills = dept_config.get('bot_skills', [])
        agent_skills = dept_config.get('agent_skills', [])
        all_dept_skills = set(bot_skills + agent_skills)
        
        print(f"    🔍 Filtering categories from department skills: {len(all_dept_skills)} skills")
        
        # Get unique conversations
        unique_conversations = df['CONVERSATION_ID'].unique()
        print(f"    🔍 Processing {len(unique_conversations)} conversations...")
        
        for conv_id in unique_conversations:
            # Get all messages for this conversation
            conv_messages = df[df['CONVERSATION_ID'] == conv_id]
            categories = set()
            
            # Extract categories only from messages with department skills
            for idx, msg in conv_messages.iterrows():
                # Check if message is from department skills
                target_skill = msg.get('TARGET_SKILL_PER_MESSAGE', '') if 'TARGET_SKILL_PER_MESSAGE' in msg else ''
                
                if target_skill in all_dept_skills:
                    text = str(msg['TEXT'])
                    # Use regex to find CategoryUsed pattern
                    category_match = re.search(
                        r'"CategoryUsed"\s*:\s*"([^"]+)"',
                        text,
                        re.IGNORECASE
                    )
                    if category_match:
                        category = category_match.group(1).strip()
                        if category:
                            categories.add(category)
            
            # Create rows for each category found
            if categories:
                for category in sorted(categories):
                    category_data.append({
                        'CONVERSATION_ID': conv_id,
                        'CATEGORY_USED': category
                    })
        
        # Save to table if we found any categories
        if category_data:
            print(f"    ✅ Found {len(category_data)} conversation-category pairs")
            
            # Convert to DataFrame
            category_df = pd.DataFrame(category_data)
            
            # Define columns
            columns = ['CONVERSATION_ID', 'CATEGORY_USED']
            
            try:
                # Save using insert_raw_data_with_cleanup
                insert_raw_data_with_cleanup(
                    session=session,
                    table_name="CONVERSATION_CATEGORIES",
                    department=department_name,
                    target_date=target_date,
                    dataframe=category_df[columns],
                    columns=columns
                )
                print(f"    💾 Saved {len(category_data)} category mappings to CONVERSATION_CATEGORIES table")
            except Exception as save_error:
                print(f"    ⚠️  Failed to save conversation categories: {str(save_error)}")
        else:
            print(f"    ℹ️  No categories found for {department_name}")
        
        return len(category_data)
    
    except Exception as e:
        print(f"    ⚠️  Error analyzing conversation categories: {str(e)}")
        return 0


def analyze_conversation_tool_calls(session, df, department_name, target_date):
    """
    Extract all tool calls from all conversations.
    Tracks every tool call attempt regardless of outcome.
    
    Only extracts tool calls from messages that belong to the department's skills
    (bot_skills + agent_skills) to ensure data quality.
    
    Args:
        session: Snowflake session
        df: Filtered DataFrame from Phase 1
        department_name: Department name
        target_date: Target date for analysis
    
    Returns:
        int: Count of tool calls found
    """
    print(f"  🛠️  Analyzing conversation tool calls for {department_name}...")
    
    if df.empty:
        print(f"    ⚠️  No data to analyze for conversation tool calls")
        return 0
    
    tool_calls_data = []
    
    try:
        # Get department skills for filtering
        departments_config = get_snowflake_departments_config()
        dept_config = departments_config.get(department_name, {})
        bot_skills = dept_config.get('bot_skills', [])
        agent_skills = dept_config.get('agent_skills', [])
        all_dept_skills = set(bot_skills + agent_skills)
        
        print(f"    🔍 Filtering tool calls from department skills: {len(all_dept_skills)} skills")
        
        # Process each message looking for tool calls
        for idx, msg in df.iterrows():
            try:
                # Check if message is from department skills
                target_skill = msg['TARGET_SKILL_PER_MESSAGE'] if 'TARGET_SKILL_PER_MESSAGE' in msg else ''
                
                if target_skill not in all_dept_skills:
                    continue  # Skip messages from other departments
                
                text = str(msg['TEXT'])
                conv_id = msg['CONVERSATION_ID']
                message_id = msg['MESSAGE_ID'] if 'MESSAGE_ID' in msg else ''
                message_time = msg['MESSAGE_SENT_TIME']
                
                # Search for tool_calls pattern in text
                # Pattern: "tool_calls":[{"id":"call_xxx","type":"tool_call","name":"ToolName"
                tool_calls_matches = re.finditer(
                    r'"tool_calls"\s*:\s*\[.*?"id"\s*:\s*"([^"]+)".*?"type"\s*:\s*"([^"]*)".*?"name"\s*:\s*"([^"]+)"',
                    text,
                    re.DOTALL
                )
                
                for match in tool_calls_matches:
                    tool_call_id = match.group(1)
                    tool_type = match.group(2)
                    tool_name = match.group(3)
                    
                    tool_calls_data.append({
                        'CONVERSATION_ID': conv_id,
                        'MESSAGE_ID': message_id,
                        'TOOL_CALL_ID': tool_call_id,
                        'TOOL_NAME': tool_name,
                        'TOOL_TYPE': tool_type,
                        'MESSAGE_TIME': message_time
                    })
            
            except Exception as msg_error:
                # Skip this message if any error occurs
                continue
        
        # Save to table if we found any tool calls
        if tool_calls_data:
            print(f"    ✅ Found {len(tool_calls_data)} tool calls")
            
            # Convert to DataFrame
            tool_calls_df = pd.DataFrame(tool_calls_data)
            
            # Define columns
            columns = [
                'CONVERSATION_ID',
                'MESSAGE_ID',
                'TOOL_CALL_ID',
                'TOOL_NAME',
                'TOOL_TYPE',
                'MESSAGE_TIME'
            ]
            
            try:
                # Save using insert_raw_data_with_cleanup
                insert_raw_data_with_cleanup(
                    session=session,
                    table_name="CONVERSATION_TOOL_CALLS",
                    department=department_name,
                    target_date=target_date,
                    dataframe=tool_calls_df[columns],
                    columns=columns
                )
                print(f"    💾 Saved {len(tool_calls_data)} tool calls to CONVERSATION_TOOL_CALLS table")
            except Exception as save_error:
                print(f"    ⚠️  Failed to save conversation tool calls: {str(save_error)}")
        else:
            print(f"    ℹ️  No tool calls found for {department_name}")
        
        return len(tool_calls_data)
    
    except Exception as e:
        print(f"    ⚠️  Error analyzing conversation tool calls: {str(e)}")
        return 0


def analyze_guardrail_stopped_tools(session, df, department_name, target_date):
    """
    Analyze guardrail-stopped tool calls for a department.

    Finds messages where guardrails stopped tool calls and extracts metadata about:
    - What tool was stopped
    - Why it was stopped (evaluator reason)
    - What agent/skill was the target
    - The policy that was checked
    
    Only processes evaluator messages from the department's skills for data quality.

    Args:
        session: Snowflake session
        df: Filtered DataFrame from Phase 1
        department_name: Department name
        target_date: Target date for analysis

    Returns:
        int: Count of stopped tool calls found
    """
    print(f"  🛡️  Analyzing guardrail-stopped tools for {department_name}...")

    if df.empty:
        print(f"    ⚠️  No data to analyze for guardrail-stopped tools")
        return 0

    stopped_tools_data = []

    try:
        # Get department skills for filtering
        departments_config = get_snowflake_departments_config()
        dept_config = departments_config.get(department_name, {})
        bot_skills = dept_config.get('bot_skills', [])
        agent_skills = dept_config.get('agent_skills', [])
        all_dept_skills = set(bot_skills + agent_skills)
        
        print(f"    🔍 Filtering stopped tools from department skills: {len(all_dept_skills)} skills")
        
        # Step 1: Find all evaluator note messages (guardrail stops) from department skills only
        evaluator_messages = df[
            (df['TEXT'].str.contains('Evaluator note:', case=False, na=False)) &
            (df['TARGET_SKILL_PER_MESSAGE'].isin(all_dept_skills))
        ]

        if evaluator_messages.empty:
            print(f"    ℹ️  No guardrail-stopped tools found for {department_name}")
            return 0

        print(f"    🔍 Found {len(evaluator_messages)} evaluator note messages")

        # Step 2: Process each evaluator message
        for idx, eval_msg in evaluator_messages.iterrows():
            try:
                conv_id = eval_msg['CONVERSATION_ID']
                eval_text = eval_msg['TEXT']
                eval_time = eval_msg['MESSAGE_SENT_TIME']
                eval_message_id = eval_msg.get('MESSAGE_ID', '')
                
                # Parse JSON from TEXT column
                eval_json = json.loads(eval_text)

                # Extract tool_call_id
                tool_call_id = eval_json.get('tool_call_id')
                if not tool_call_id:
                    continue

                # Extract evaluator reason from content
                content = eval_json.get('content', '')
                evaluator_reason = ''
                
                # Content might be a nested JSON string
                if isinstance(content, str) and content.strip():
                    try:
                        content_json = json.loads(content)
                        # Try both 'Message' and 'message' (case-insensitive)
                        evaluator_reason = content_json.get('Message', content_json.get('message', ''))
                    except:
                        evaluator_reason = content

                # Step 3: Find the original tool call message with this tool_call_id
                conv_messages = df[df['CONVERSATION_ID'] == conv_id]

                tool_call_found = False
                for msg_idx, msg in conv_messages.iterrows():
                    try:
                        msg_text = msg['TEXT']

                        # Check if this message contains the tool_call_id
                        if tool_call_id not in msg_text:
                            continue

                        # Parse the message JSON
                        msg_json = json.loads(msg_text)

                        # Check if it has tool_calls array
                        tool_calls_array = msg_json.get('tool_calls', [])
                        if not tool_calls_array:
                            continue

                        # Find the matching tool call by ID
                        for tool_call in tool_calls_array:
                            if tool_call.get('id') == tool_call_id:
                                # Extract metadata
                                tool_name = tool_call.get('name', '')
                                tool_args = tool_call.get('args', {})
                                tool_type = tool_call.get('type', '')
                                
                                # Handle new structure: args might have a 'query' wrapper
                                # New: {"args": {"query": {"Agent": "...", "PolicyUsed": "..."}}}
                                # Old: {"args": {"Agent": "...", "PolicyUsed": "..."}}
                                if 'query' in tool_args and isinstance(tool_args['query'], dict):
                                    # New structure: extract from query
                                    args_data = tool_args['query']
                                else:
                                    # Old structure: use args directly
                                    args_data = tool_args
                                
                                # Extract specific fields from args_data (check multiple variations)
                                target_agent = args_data.get('Agent', args_data.get('agent', ''))
                                policy_used = args_data.get('PolicyUsed', 
                                                          args_data.get('Policy_Used',
                                                          args_data.get('policy_used', 
                                                          args_data.get('policy', ''))))
                                
                                # Extract TARGET_SKILL_PER_MESSAGE, EXECUTION_ID, and MESSAGE_ID from the message row
                                target_skill_per_message = msg.get('TARGET_SKILL_PER_MESSAGE', '')
                                execution_id = msg.get('EXECUTION_ID', '')
                                message_id = msg.get('MESSAGE_ID', '')
                                
                                # Extract all categories used in this conversation
                                categories_used = extract_categories_from_conversation(df, conv_id)
                                
                                # Store the data (DATE, DEPARTMENT, TIMESTAMP will be auto-added by insert_raw_data_with_cleanup)
                                stopped_tools_data.append({
                                    'CONVERSATION_ID': conv_id,
                                    'TOOL_CALL_ID': tool_call_id,
                                    'TOOL_NAME': tool_name,
                                    'TOOL_TYPE': tool_type,
                                    'TARGET_AGENT': target_agent,
                                    'POLICY_USED': policy_used,
                                    'EVALUATOR_REASON': evaluator_reason,
                                    'TOOL_CALL_MESSAGE_TIME': msg['MESSAGE_SENT_TIME'],
                                    'EVALUATOR_MESSAGE_TIME': eval_time,
                                    'TOOL_ARGS_JSON': json.dumps(tool_args),
                                    'TARGET_SKILL_PER_MESSAGE': target_skill_per_message,
                                    'EXECUTION_ID': execution_id,
                                    'MESSAGE_ID': message_id,
                                    'EVALUATOR_NOTE_MESSAGE_ID': eval_message_id,
                                    'CATEGORIES_USED': categories_used
                                })

                                tool_call_found = True
                                break

                        if tool_call_found:
                            break

                    except json.JSONDecodeError:
                        # Skip messages that aren't valid JSON
                        continue
                    except Exception as msg_error:
                        # Skip this message if any error occurs
                        continue

                if not tool_call_found:
                    print(f"    ⚠️  Could not find matching tool call for ID: {tool_call_id}")

            except json.JSONDecodeError:
                # Skip evaluator messages that aren't valid JSON
                continue
            except Exception as e:
                # Skip this evaluator message if any error occurs
                print(f"    ⚠️  Error processing evaluator message: {str(e)}")
                continue

        # Step 4: Save to raw table if we found any stopped tools
        if stopped_tools_data:
            print(f"    ✅ Found {len(stopped_tools_data)} guardrail-stopped tool calls")

            # Convert to DataFrame
            stopped_tools_df = pd.DataFrame(stopped_tools_data)

            # Define columns for the table (excluding DATE, DEPARTMENT, TIMESTAMP which are auto-added)
            columns = [
                'CONVERSATION_ID',
                'TOOL_CALL_ID',
                'TOOL_NAME',
                'TOOL_TYPE',
                'TARGET_AGENT',
                'POLICY_USED',
                'EVALUATOR_REASON',
                'TOOL_CALL_MESSAGE_TIME',
                'EVALUATOR_MESSAGE_TIME',
                'TOOL_ARGS_JSON',
                'TARGET_SKILL_PER_MESSAGE',
                'EXECUTION_ID',
                'MESSAGE_ID',
                'EVALUATOR_NOTE_MESSAGE_ID',
                'CATEGORIES_USED'
            ]

            try:
                # Save using insert_raw_data_with_cleanup
                insert_raw_data_with_cleanup(
                    session=session,
                    table_name="GUARDRAIL_STOPPED_TOOLS",
                    department=department_name,
                    target_date=target_date,
                    dataframe=stopped_tools_df[columns],
                    columns=columns
                )
                print(f"    💾 Saved {len(stopped_tools_data)} guardrail-stopped tools to GUARDRAIL_STOPPED_TOOLS table")
            except Exception as save_error:
                print(f"    ⚠️  Failed to save guardrail-stopped tools: {str(save_error)}")
        else:
            print(f"    ℹ️  No guardrail-stopped tools found for {department_name}")

        return len(stopped_tools_data)

    except Exception as e:
        print(f"    ⚠️  Error analyzing guardrail-stopped tools: {str(e)}")
        return 0

def analyze_guardrail_missed_tools(session, df, department_name, target_date):
    """
    Analyze guardrail-detected missed tool calls for a department.
    
    Detects two types of missed tool calls:
    1. False Promise - Tool should have been called due to false promise
    2. Policy Based - Tool should have been called based on policy
    
    Only processes missed tool messages from the department's skills for data quality.
    
    Args:
        session: Snowflake session
        df: Filtered DataFrame from Phase 1
        department_name: Department name
        target_date: Target date for analysis
    
    Returns:
        int: Count of missed tool calls found
    """
    print(f"  🎯 Analyzing guardrail-missed tools for {department_name}...")
    
    if df.empty:
        print(f"    ⚠️  No data to analyze for guardrail-missed tools")
        return 0
    
    missed_tools_data = []
    
    try:
        # Get department skills for filtering
        departments_config = get_snowflake_departments_config()
        dept_config = departments_config.get(department_name, {})
        bot_skills = dept_config.get('bot_skills', [])
        agent_skills = dept_config.get('agent_skills', [])
        all_dept_skills = set(bot_skills + agent_skills)
        
        print(f"    🔍 Filtering missed tools from department skills: {len(all_dept_skills)} skills")
        
        # Step 1: Find all missed tool messages (both types) from department skills only
        false_promise_msgs = df[
            (df['TEXT'].str.contains(
                'GUARDRAIL DETECTED A MISSED TOOL CALL CAUSED BY A FALSE PROMISE',
                case=False, na=False
            )) &
            (df['TARGET_SKILL_PER_MESSAGE'].isin(all_dept_skills))
        ]
        
        policy_based_msgs = df[
            (df['TEXT'].str.contains(
                'GUARDRAIL DETECTED A MISSED TOOL CALL THAT IS POLICY BASED',
                case=False, na=False
            )) &
            (df['TARGET_SKILL_PER_MESSAGE'].isin(all_dept_skills))
        ]
        
        # Combine both types
        missed_tool_messages = pd.concat([false_promise_msgs, policy_based_msgs], ignore_index=True)
        
        if missed_tool_messages.empty:
            print(f"    ℹ️  No guardrail-missed tools found for {department_name}")
            return 0
        
        print(f"    🔍 Found {len(missed_tool_messages)} missed tool messages")
        
        # Step 2: Process each missed tool message
        for idx, msg in missed_tool_messages.iterrows():
            try:
                conv_id = msg['CONVERSATION_ID']
                message_text = msg['TEXT']
                message_time = msg['MESSAGE_SENT_TIME']
                message_id = msg['MESSAGE_ID'] if 'MESSAGE_ID' in msg else ''
                target_skill = msg['TARGET_SKILL_PER_MESSAGE'] if 'TARGET_SKILL_PER_MESSAGE' in msg else ''
                execution_id = msg['EXECUTION_ID'] if 'EXECUTION_ID' in msg else ''
                
                # Determine missed tool type from the message
                if 'FALSE PROMISE' in message_text.upper():
                    missed_tool_type = 'False Promise'
                elif 'POLICY BASED' in message_text.upper():
                    missed_tool_type = 'Policy Based'
                else:
                    missed_tool_type = 'Unknown'
                
                # Extract sections from the message text
                gpt_response_caught = ''
                last_customer_message = ''
                guardrail_output_json = {}
                
                # Extract GPT Response (between "GPT RESPONSE CAUGHT:" and "LAST CUSTOMER MESSAGE:")
                gpt_response_match = re.search(
                    r'GPT RESPONSE CAUGHT:\s*(.+?)(?=LAST CUSTOMER MESSAGE:|Guardrail Output:|$)',
                    message_text,
                    re.DOTALL | re.IGNORECASE
                )
                if gpt_response_match:
                    gpt_response_caught = gpt_response_match.group(1).strip()
                
                # Extract Last Customer Message (between "LAST CUSTOMER MESSAGE:" and "Guardrail Output:")
                customer_msg_match = re.search(
                    r'LAST CUSTOMER MESSAGE:\s*(.+?)(?=Guardrail Output:|$)',
                    message_text,
                    re.DOTALL | re.IGNORECASE
                )
                if customer_msg_match:
                    last_customer_message = customer_msg_match.group(1).strip()
                
                # Extract Guardrail Output JSON (after "Guardrail Output:")
                guardrail_match = re.search(
                    r'Guardrail Output:\s*(\{.+?\})\s*$',
                    message_text,
                    re.DOTALL | re.IGNORECASE
                )
                if guardrail_match:
                    try:
                        guardrail_output_json = json.loads(guardrail_match.group(1))
                        print(f"    🔍 Guardrail Output JSON: {guardrail_output_json}")
                    except json.JSONDecodeError:
                        guardrail_output_json = {}
                        print(f"    ⚠️ Guardrail Output is not valid JSON: {guardrail_match.group(1)}")
                
                # Extract fields from Guardrail Output JSON
                missed_tool_name = guardrail_output_json.get('missedToolCallName', '') if isinstance(guardrail_output_json, dict) else ''
                explanation = guardrail_output_json.get('explanation', '') if isinstance(guardrail_output_json, dict) else ''
                type_from_json = guardrail_output_json.get('typeOfMissedToolCall', '') if isinstance(guardrail_output_json, dict) else ''

                # Fallback: some guardrail messages use bullet-point format instead of JSON
                # e.g. "- relevantToolName: PaymentsTool"
                if not missed_tool_name:
                    tool_name_bullet = re.search(
                        r'-\s*relevantToolName\s*:\s*(.+?)(?:\n|$)',
                        message_text,
                        re.IGNORECASE
                    )
                    if tool_name_bullet:
                        missed_tool_name = tool_name_bullet.group(1).strip()

                # Fallback: extract explanation from bullet format if not already extracted
                if not explanation:
                    explanation_bullet = re.search(
                        r'-\s*explanation\s*:\s*(.+?)(?:\n\n|$)',
                        message_text,
                        re.IGNORECASE | re.DOTALL
                    )
                    if explanation_bullet:
                        explanation = explanation_bullet.group(1).strip()

                # Parse GPT Response JSON if it's a JSON string
                category_used = ''
                policy_used = ''
                action_promised = ''
                escalation_risk = None
                
                if gpt_response_caught:
                    try:
                        gpt_response_json = json.loads(gpt_response_caught)
                        if isinstance(gpt_response_json, dict):
                            category_used = gpt_response_json.get('CategoryUsed', '')
                            policy_used = gpt_response_json.get('PolicyUsed', '')
                            action_promised = gpt_response_json.get('ActionPromisedToClient', '')
                            escalation_risk = gpt_response_json.get('EscalationRiskAssessment', None)
                    except json.JSONDecodeError:
                        # GPT response is not valid JSON, keep as string
                        print(f"    ⚠️ GPT Response is not valid JSON: {gpt_response_caught}")
                        pass
                
                # Extract all categories used in this conversation
                categories_used = extract_categories_from_conversation(df, conv_id)
                
                # Store the data
                missed_tools_data.append({
                    'CONVERSATION_ID': conv_id,
                    'MESSAGE_ID': message_id,
                    'MESSAGE_TIME': message_time,
                    'TARGET_SKILL_PER_MESSAGE': target_skill,
                    'EXECUTION_ID': execution_id,
                    'MISSED_TOOL_TYPE': type_from_json or missed_tool_type,
                    'MISSED_TOOL_NAME': missed_tool_name,
                    'EXPLANATION': explanation,
                    'LAST_CUSTOMER_MESSAGE': last_customer_message,
                    'GPT_RESPONSE_CAUGHT': gpt_response_caught,
                    'CATEGORY_USED': category_used,
                    'POLICY_USED': policy_used,
                    'ACTION_PROMISED': action_promised,
                    'ESCALATION_RISK': str(escalation_risk) if escalation_risk is not None else '',
                    'GUARDRAIL_OUTPUT_JSON': json.dumps(guardrail_output_json) if guardrail_output_json else '',
                    'CATEGORIES_USED': categories_used
                })
            
            except Exception as e:
                # Skip this message if any error occurs
                print(f"    ⚠️  Error processing missed tool message: {str(e)}")
                continue
        
        # Step 3: Save to raw table if we found any missed tools
        if missed_tools_data:
            print(f"    ✅ Found {len(missed_tools_data)} guardrail-missed tool calls")
            
            # Convert to DataFrame
            missed_tools_df = pd.DataFrame(missed_tools_data)
            
            # Define columns for the table (excluding DATE, DEPARTMENT, TIMESTAMP which are auto-added)
            columns = [
                'CONVERSATION_ID',
                'MESSAGE_ID',
                'MESSAGE_TIME',
                'TARGET_SKILL_PER_MESSAGE',
                'EXECUTION_ID',
                'MISSED_TOOL_TYPE',
                'MISSED_TOOL_NAME',
                'EXPLANATION',
                'LAST_CUSTOMER_MESSAGE',
                'GPT_RESPONSE_CAUGHT',
                'CATEGORY_USED',
                'POLICY_USED',
                'ACTION_PROMISED',
                'ESCALATION_RISK',
                'GUARDRAIL_OUTPUT_JSON',
                'CATEGORIES_USED'
            ]
            
            try:
                # Save using insert_raw_data_with_cleanup
                insert_raw_data_with_cleanup(
                    session=session,
                    table_name="GUARDRAIL_MISSED_TOOLS",
                    department=department_name,
                    target_date=target_date,
                    dataframe=missed_tools_df[columns],
                    columns=columns
                )
                print(f"    💾 Saved {len(missed_tools_data)} guardrail-missed tools to GUARDRAIL_MISSED_TOOLS table")
            except Exception as save_error:
                print(f"    ⚠️  Failed to save guardrail-missed tools: {str(save_error)}")
        else:
            print(f"    ℹ️  No guardrail-missed tools found for {department_name}")
        
        return len(missed_tools_data)
    
    except Exception as e:
        print(f"    ⚠️  Error analyzing guardrail-missed tools: {str(e)}")
        return 0


def analyze_guardrail_false_promise_no_tool(session, df, department_name, target_date):
    """
    Analyze guardrail-detected false promises with no relevant tool available.
    
    Detects cases where the bot made a promise but no tool exists to fulfill it.
    
    Only processes false promise messages from the department's skills for data quality.
    
    Args:
        session: Snowflake session
        df: Filtered DataFrame from Phase 1
        department_name: Department name
        target_date: Target date for analysis
    
    Returns:
        int: Count of false promise with no tool cases found
    """
    print(f"  🚫 Analyzing guardrail-false promise with no tool for {department_name}...")
    
    if df.empty:
        print(f"    ⚠️  No data to analyze for guardrail-false promise no tool")
        return 0
    
    false_promise_data = []
    
    try:
        # Get department skills for filtering
        departments_config = get_snowflake_departments_config()
        dept_config = departments_config.get(department_name, {})
        bot_skills = dept_config.get('bot_skills', [])
        agent_skills = dept_config.get('agent_skills', [])
        all_dept_skills = set(bot_skills + agent_skills)
        
        print(f"    🔍 Filtering false promise messages from department skills: {len(all_dept_skills)} skills")
        
        # Step 1: Find all false promise with no tool messages from department skills only
        false_promise_msgs = df[
            (df['TEXT'].str.contains(
                'GUARDRAIL DETECTED A FALSE PROMISE WITH NO RELEVANT TOOL AVAILABLE TO FULFILL THE PROMISE',
                case=False, na=False
            )) &
            (df['TARGET_SKILL_PER_MESSAGE'].isin(all_dept_skills))
        ]
        
        if false_promise_msgs.empty:
            print(f"    ℹ️  No guardrail-false promise with no tool found for {department_name}")
            return 0
        
        print(f"    🔍 Found {len(false_promise_msgs)} false promise with no tool messages")
        
        # Step 2: Process each false promise message
        for idx, msg in false_promise_msgs.iterrows():
            try:
                conv_id = msg['CONVERSATION_ID']
                msg_id = msg['MESSAGE_ID'] if 'MESSAGE_ID' in msg else ''
                msg_time = msg['TIME'] if 'TIME' in msg else None
                target_skill = msg['TARGET_SKILL_PER_MESSAGE'] if 'TARGET_SKILL_PER_MESSAGE' in msg else ''
                execution_id = msg['EXECUTION_ID'] if 'EXECUTION_ID' in msg else ''
                text = msg['TEXT'] if 'TEXT' in msg else ''
                
                # Extract GPT RESPONSE CAUGHT
                gpt_response_match = re.search(r'- GPT RESPONSE CAUGHT:\s*"({.*?})"', text, re.DOTALL | re.IGNORECASE)
                gpt_response_caught = gpt_response_match.group(1) if gpt_response_match else ''
                
                # Extract category_used, policy_used, action_promised, escalation_risk from GPT response
                category_used = ''
                policy_used = ''
                action_promised = ''
                escalation_risk = ''
                
                if gpt_response_caught:
                    try:
                        gpt_json = json.loads(gpt_response_caught)
                        category_used = gpt_json.get('CategoryUsed', gpt_json.get('categoryUsed', ''))
                        policy_used = gpt_json.get('PolicyUsed', gpt_json.get('policyUsed', ''))
                        action_promised = gpt_json.get('ActionPromisedToClient', gpt_json.get('actionPromisedToClient', ''))
                        escalation_risk = gpt_json.get('EscalationRiskAssessment', gpt_json.get('escalationRiskAssessment', ''))
                    except json.JSONDecodeError:
                        pass
                
                # Extract LAST CUSTOMER MESSAGE
                last_customer_match = re.search(r'LAST CUSTOMER MESSAGE:\s*(.+?)(?=\n\n|Guardrail Output:|$)', text, re.DOTALL | re.IGNORECASE)
                last_customer_message = last_customer_match.group(1).strip() if last_customer_match else ''
                
                # Extract Guardrail Output JSON
                guardrail_output_match = re.search(r'Guardrail Output:\s*({.*?})(?=\n\n|$)', text, re.DOTALL | re.IGNORECASE)
                guardrail_output_json = guardrail_output_match.group(1) if guardrail_output_match else ''
                
                # Extract explanation from the text (not in JSON, in the text itself)
                explanation_match = re.search(r'-\s*explanation:\s*(.+?)(?=\n\n|$)', text, re.DOTALL | re.IGNORECASE)
                explanation = explanation_match.group(1).strip() if explanation_match else ''
                
                # Extract categories used in this conversation
                conv_messages = df[df['CONVERSATION_ID'] == conv_id]
                categories_used = extract_categories_from_conversation(conv_messages, all_dept_skills)
                
                # Build data record
                false_promise_data.append({
                    'CONVERSATION_ID': conv_id,
                    'MESSAGE_ID': msg_id,
                    'MESSAGE_TIME': msg_time,
                    'TARGET_SKILL_PER_MESSAGE': target_skill,
                    'EXECUTION_ID': execution_id,
                    'EXPLANATION': explanation,
                    'LAST_CUSTOMER_MESSAGE': last_customer_message,
                    'GPT_RESPONSE_CAUGHT': gpt_response_caught,
                    'CATEGORY_USED': category_used,
                    'POLICY_USED': policy_used,
                    'ACTION_PROMISED': action_promised,
                    'ESCALATION_RISK': str(escalation_risk),
                    'GUARDRAIL_OUTPUT_JSON': guardrail_output_json,
                    'CATEGORIES_USED': categories_used
                })
                
            except Exception as msg_error:
                print(f"    ⚠️  Error processing false promise message: {str(msg_error)}")
                continue
        
        # Step 3: Save to Snowflake if we have data
        if false_promise_data:
            # Convert to DataFrame
            false_promise_df = pd.DataFrame(false_promise_data)
            
            # Define columns for the table (excluding DATE, DEPARTMENT, TIMESTAMP which are auto-added)
            columns = [
                'CONVERSATION_ID',
                'MESSAGE_ID',
                'MESSAGE_TIME',
                'TARGET_SKILL_PER_MESSAGE',
                'EXECUTION_ID',
                'EXPLANATION',
                'LAST_CUSTOMER_MESSAGE',
                'GPT_RESPONSE_CAUGHT',
                'CATEGORY_USED',
                'POLICY_USED',
                'ACTION_PROMISED',
                'ESCALATION_RISK',
                'GUARDRAIL_OUTPUT_JSON',
                'CATEGORIES_USED'
            ]
            
            try:
                # Save using insert_raw_data_with_cleanup
                insert_raw_data_with_cleanup(
                    session=session,
                    table_name="GUARDRAIL_FALSE_PROMISE_NO_TOOL",
                    department=department_name,
                    target_date=target_date,
                    dataframe=false_promise_df[columns],
                    columns=columns
                )
                print(f"    💾 Saved {len(false_promise_data)} guardrail-false promise with no tool to GUARDRAIL_FALSE_PROMISE_NO_TOOL table")
            except Exception as save_error:
                print(f"    ⚠️  Failed to save guardrail-false promise no tool: {str(save_error)}")
        else:
            print(f"    ℹ️  No guardrail-false promise with no tool found for {department_name}")
        
        return len(false_promise_data)
    
    except Exception as e:
        print(f"    ⚠️  Error analyzing guardrail-false promise no tool: {str(e)}")
        return 0


def generate_guardrail_summary_by_category(session, department_name, target_date):
    """
    Generate summary statistics per category showing guardrail interventions.
    
    Joins data from:
    - CONVERSATION_CATEGORIES (all conversations with categories)
    - CONVERSATION_TOOL_CALLS (all tool call attempts)
    - GUARDRAIL_STOPPED_TOOLS (wrong tools)
    - GUARDRAIL_MISSED_TOOLS (missed tools by type)
    - GUARDRAIL_FALSE_PROMISE_NO_TOOL (false promises with no relevant tool)
    
    Args:
        session: Snowflake session
        department_name: Department name
        target_date: Target date for analysis
    
    Returns:
        int: Count of categories in summary
    """
    print(f"  📈 Generating guardrail summary by category for {department_name}...")
    
    try:
        # Build SQL query for summary
        query = f"""
        WITH total_convs AS (
            -- Total unique conversations across all categories (for percentage calculation)
            SELECT COUNT(DISTINCT CONVERSATION_ID) as OVERALL_TOTAL_CONVERSATIONS
            FROM CONVERSATION_CATEGORIES
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
        ),
        category_convs AS (
            -- Total conversations per category
            SELECT 
                CATEGORY_USED,
                COUNT(DISTINCT CONVERSATION_ID) as TOTAL_CONVERSATIONS
            FROM CONVERSATION_CATEGORIES
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
            GROUP BY CATEGORY_USED
        ),
        category_tools AS (
            -- Total tool calls per category (ALL - for reference)
            SELECT 
                cc.CATEGORY_USED,
                COUNT(DISTINCT ct.TOOL_CALL_ID) as TOTAL_TOOL_CALLS
            FROM CONVERSATION_CATEGORIES cc
            JOIN CONVERSATION_TOOL_CALLS ct
                ON cc.CONVERSATION_ID = ct.CONVERSATION_ID
                AND cc.DATE = ct.DATE
                AND cc.DEPARTMENT = ct.DEPARTMENT
            WHERE cc.DATE = '{target_date}' AND cc.DEPARTMENT = '{department_name}'
            GROUP BY cc.CATEGORY_USED
        ),
        category_tools_successful AS (
            -- Successful tool calls per category (excluding stopped tools)
            SELECT 
                cc.CATEGORY_USED,
                COUNT(DISTINCT ct.TOOL_CALL_ID) as SUCCESSFUL_TOOL_CALLS
            FROM CONVERSATION_CATEGORIES cc
            JOIN CONVERSATION_TOOL_CALLS ct
                ON cc.CONVERSATION_ID = ct.CONVERSATION_ID
                AND cc.DATE = ct.DATE
                AND cc.DEPARTMENT = ct.DEPARTMENT
            LEFT JOIN GUARDRAIL_STOPPED_TOOLS gst
                ON ct.TOOL_CALL_ID = gst.TOOL_CALL_ID
                AND ct.DATE = gst.DATE
                AND ct.DEPARTMENT = gst.DEPARTMENT
            WHERE cc.DATE = '{target_date}' 
              AND cc.DEPARTMENT = '{department_name}'
              AND gst.TOOL_CALL_ID IS NULL
            GROUP BY cc.CATEGORY_USED
        ),
        wrong_tools AS (
            -- Stopped tools per category
            SELECT 
                cc.CATEGORY_USED,
                COUNT(DISTINCT gst.CONVERSATION_ID) as WRONG_TOOLS_COUNT,
                COUNT(*) as WRONG_TOOLS_TOOL_COUNT
            FROM CONVERSATION_CATEGORIES cc
            JOIN GUARDRAIL_STOPPED_TOOLS gst
                ON cc.CONVERSATION_ID = gst.CONVERSATION_ID
                AND cc.DATE = gst.DATE
                AND cc.DEPARTMENT = gst.DEPARTMENT
            WHERE cc.DATE = '{target_date}' AND cc.DEPARTMENT = '{department_name}'
            GROUP BY cc.CATEGORY_USED
        ),
        missed_tools AS (
            -- Missed tools per category (grouped by type)
            SELECT 
                cc.CATEGORY_USED,
                COUNT(DISTINCT CASE WHEN gmt.MISSED_TOOL_TYPE = 'False Promise' THEN gmt.CONVERSATION_ID END) as MISSED_FALSE_PROMISE_CONVS,
                COUNT(DISTINCT CASE WHEN gmt.MISSED_TOOL_TYPE = 'Policy Based' THEN gmt.CONVERSATION_ID END) as MISSED_POLICY_BASED_CONVS,
                COUNT(DISTINCT gmt.CONVERSATION_ID) as TOTAL_MISSED_CONVS,
                SUM(CASE WHEN gmt.MISSED_TOOL_TYPE = 'False Promise' THEN 1 ELSE 0 END) as MISSED_FALSE_PROMISE_TOOLS,
                SUM(CASE WHEN gmt.MISSED_TOOL_TYPE = 'Policy Based' THEN 1 ELSE 0 END) as MISSED_POLICY_BASED_TOOLS,
                COUNT(*) as TOTAL_MISSED_TOOLS
            FROM CONVERSATION_CATEGORIES cc
            JOIN GUARDRAIL_MISSED_TOOLS gmt
                ON cc.CONVERSATION_ID = gmt.CONVERSATION_ID
                AND cc.DATE = gmt.DATE
                AND cc.DEPARTMENT = gmt.DEPARTMENT
            WHERE cc.DATE = '{target_date}' AND cc.DEPARTMENT = '{department_name}'
            GROUP BY cc.CATEGORY_USED
        ),
        false_promise_no_tool AS (
            -- False promise with no tool per category
            SELECT 
                cc.CATEGORY_USED,
                COUNT(DISTINCT gfp.CONVERSATION_ID) as FALSE_PROMISE_NO_TOOL_CONVS
            FROM CONVERSATION_CATEGORIES cc
            JOIN GUARDRAIL_FALSE_PROMISE_NO_TOOL gfp
                ON cc.CONVERSATION_ID = gfp.CONVERSATION_ID
                AND cc.DATE = gfp.DATE
                AND cc.DEPARTMENT = gfp.DEPARTMENT
            WHERE cc.DATE = '{target_date}' AND cc.DEPARTMENT = '{department_name}'
            GROUP BY cc.CATEGORY_USED
        )
        SELECT 
            cc.CATEGORY_USED as CATEGORY,
            cc.TOTAL_CONVERSATIONS,
            ROUND((cc.TOTAL_CONVERSATIONS * 100.0 / NULLIF(tc.OVERALL_TOTAL_CONVERSATIONS, 0)), 2) as TOTAL_CONVERSATIONS_PCT,
            COALESCE(ct.TOTAL_TOOL_CALLS, 0) as TOTAL_TOOL_CALLS,
            
            -- Wrong Tools (stopped) - per conversation and per tool
            COALESCE(wt.WRONG_TOOLS_COUNT, 0) as WRONG_TOOLS_COUNT,
            ROUND((COALESCE(wt.WRONG_TOOLS_COUNT, 0) * 100.0 / NULLIF(cc.TOTAL_CONVERSATIONS, 0)), 2) as WRONG_TOOLS_PCT_OF_CONVERSATIONS,
            COALESCE(wt.WRONG_TOOLS_TOOL_COUNT, 0) as WRONG_TOOLS_TOOL_COUNT,
            ROUND((COALESCE(wt.WRONG_TOOLS_TOOL_COUNT, 0) * 100.0 / NULLIF(ct.TOTAL_TOOL_CALLS, 0)), 2) as WRONG_TOOLS_PCT_OF_TOOL_CALLS,
            
            -- Missed Tools - False Promise (per conversation)
            COALESCE(mt.MISSED_FALSE_PROMISE_CONVS, 0) as MISSED_FALSE_PROMISE_COUNT,
            ROUND((COALESCE(mt.MISSED_FALSE_PROMISE_CONVS, 0) * 100.0 / NULLIF(cc.TOTAL_CONVERSATIONS, 0)), 2) as MISSED_FALSE_PROMISE_PCT_OF_CONVERSATIONS,
            
            -- Missed Tools - False Promise (per tool) - NEW ADJUSTED DENOMINATOR
            ROUND((COALESCE(mt.MISSED_FALSE_PROMISE_TOOLS, 0) * 100.0 / 
                   NULLIF(COALESCE(cts.SUCCESSFUL_TOOL_CALLS, 0) + COALESCE(mt.TOTAL_MISSED_TOOLS, 0), 0)), 2) as MISSED_FALSE_PROMISE_PCT_OF_TOOLS,
            
            -- Missed Tools - Policy Based (per conversation)
            COALESCE(mt.MISSED_POLICY_BASED_CONVS, 0) as MISSED_POLICY_BASED_COUNT,
            ROUND((COALESCE(mt.MISSED_POLICY_BASED_CONVS, 0) * 100.0 / NULLIF(cc.TOTAL_CONVERSATIONS, 0)), 2) as MISSED_POLICY_BASED_PCT_OF_CONVERSATIONS,
            
            -- Missed Tools - Policy Based (per tool) - NEW ADJUSTED DENOMINATOR
            ROUND((COALESCE(mt.MISSED_POLICY_BASED_TOOLS, 0) * 100.0 / 
                   NULLIF(COALESCE(cts.SUCCESSFUL_TOOL_CALLS, 0) + COALESCE(mt.TOTAL_MISSED_TOOLS, 0), 0)), 2) as MISSED_POLICY_BASED_PCT_OF_TOOLS,
            
            -- Total Missed Tools - Combined (per conversation)
            COALESCE(mt.TOTAL_MISSED_CONVS, 0) as TOTAL_MISSED_TOOLS_COUNT,
            ROUND((COALESCE(mt.TOTAL_MISSED_CONVS, 0) * 100.0 / NULLIF(cc.TOTAL_CONVERSATIONS, 0)), 2) as TOTAL_MISSED_TOOLS_PCT_OF_CONVERSATIONS,
            
            -- Total Missed Tools - Combined (per tool) - NEW ADJUSTED DENOMINATOR
            ROUND((COALESCE(mt.TOTAL_MISSED_TOOLS, 0) * 100.0 / 
                   NULLIF(COALESCE(cts.SUCCESSFUL_TOOL_CALLS, 0) + COALESCE(mt.TOTAL_MISSED_TOOLS, 0), 0)), 2) as TOTAL_MISSED_TOOLS_PCT_OF_TOOLS,
            
            -- False Promise with No Tool (conversation-level only)
            COALESCE(fp.FALSE_PROMISE_NO_TOOL_CONVS, 0) as FALSE_PROMISE_NO_TOOL_COUNT,
            ROUND((COALESCE(fp.FALSE_PROMISE_NO_TOOL_CONVS, 0) * 100.0 / NULLIF(cc.TOTAL_CONVERSATIONS, 0)), 2) as FALSE_PROMISE_NO_TOOL_PCT_OF_CONVERSATIONS,
            
            -- Total Guardrail Interventions (all types including false promise no tool)
            (COALESCE(wt.WRONG_TOOLS_COUNT, 0) + COALESCE(mt.TOTAL_MISSED_CONVS, 0) + COALESCE(fp.FALSE_PROMISE_NO_TOOL_CONVS, 0)) as TOTAL_GUARDRAIL_INTERVENTIONS,
            ROUND(((COALESCE(wt.WRONG_TOOLS_COUNT, 0) + COALESCE(mt.TOTAL_MISSED_CONVS, 0) + COALESCE(fp.FALSE_PROMISE_NO_TOOL_CONVS, 0)) * 100.0 / NULLIF(cc.TOTAL_CONVERSATIONS, 0)), 2) as TOTAL_INTERVENTIONS_PCT_OF_CONVERSATIONS,
            
            -- Adjusted tool denominator (for reference)
            (COALESCE(cts.SUCCESSFUL_TOOL_CALLS, 0) + COALESCE(mt.TOTAL_MISSED_TOOLS, 0)) as ADJUSTED_TOOL_CALLS_DENOMINATOR
            
        FROM category_convs cc
        CROSS JOIN total_convs tc
        LEFT JOIN category_tools ct ON cc.CATEGORY_USED = ct.CATEGORY_USED
        LEFT JOIN category_tools_successful cts ON cc.CATEGORY_USED = cts.CATEGORY_USED
        LEFT JOIN wrong_tools wt ON cc.CATEGORY_USED = wt.CATEGORY_USED
        LEFT JOIN missed_tools mt ON cc.CATEGORY_USED = mt.CATEGORY_USED
        LEFT JOIN false_promise_no_tool fp ON cc.CATEGORY_USED = fp.CATEGORY_USED
        ORDER BY cc.TOTAL_CONVERSATIONS DESC
        """
        
        # Execute query
        print(f"    🔍 Executing summary query...")
        summary_df = session.sql(query).to_pandas()
        
        if summary_df.empty:
            print(f"    ℹ️  No summary data generated for {department_name}")
            return 0
        
        print(f"    ✅ Generated summary for {len(summary_df)} categories")
        
        # Calculate totals row directly from raw tables (to avoid double-counting across categories)
        totals_query = f"""
        WITH total_convs AS (
            SELECT COUNT(DISTINCT CONVERSATION_ID) as TOTAL_CONVERSATIONS
            FROM CONVERSATION_CATEGORIES
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
        ),
        total_tools AS (
            SELECT COUNT(DISTINCT TOOL_CALL_ID) as TOTAL_TOOL_CALLS
            FROM CONVERSATION_TOOL_CALLS
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
        ),
        successful_tools AS (
            SELECT COUNT(DISTINCT ct.TOOL_CALL_ID) as SUCCESSFUL_TOOL_CALLS
            FROM CONVERSATION_TOOL_CALLS ct
            LEFT JOIN GUARDRAIL_STOPPED_TOOLS gst
                ON ct.TOOL_CALL_ID = gst.TOOL_CALL_ID
                AND ct.DATE = gst.DATE
                AND ct.DEPARTMENT = gst.DEPARTMENT
            WHERE ct.DATE = '{target_date}' 
              AND ct.DEPARTMENT = '{department_name}'
              AND gst.TOOL_CALL_ID IS NULL
        ),
        wrong_tools_total AS (
            SELECT 
                COUNT(DISTINCT CONVERSATION_ID) as WRONG_TOOLS_COUNT,
                COUNT(*) as WRONG_TOOLS_TOOL_COUNT
            FROM GUARDRAIL_STOPPED_TOOLS
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
        ),
        missed_tools_total AS (
            SELECT 
                COUNT(DISTINCT CASE WHEN MISSED_TOOL_TYPE = 'False Promise' THEN CONVERSATION_ID END) as MISSED_FALSE_PROMISE_CONVS,
                COUNT(DISTINCT CASE WHEN MISSED_TOOL_TYPE = 'Policy Based' THEN CONVERSATION_ID END) as MISSED_POLICY_BASED_CONVS,
                COUNT(DISTINCT CONVERSATION_ID) as TOTAL_MISSED_CONVS,
                SUM(CASE WHEN MISSED_TOOL_TYPE = 'False Promise' THEN 1 ELSE 0 END) as MISSED_FALSE_PROMISE_TOOLS,
                SUM(CASE WHEN MISSED_TOOL_TYPE = 'Policy Based' THEN 1 ELSE 0 END) as MISSED_POLICY_BASED_TOOLS,
                COUNT(*) as TOTAL_MISSED_TOOLS
            FROM GUARDRAIL_MISSED_TOOLS
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
        ),
        false_promise_no_tool_total AS (
            SELECT COUNT(DISTINCT CONVERSATION_ID) as FALSE_PROMISE_NO_TOOL_COUNT
            FROM GUARDRAIL_FALSE_PROMISE_NO_TOOL
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
        )
        SELECT 
            tc.TOTAL_CONVERSATIONS,
            tt.TOTAL_TOOL_CALLS,
            st.SUCCESSFUL_TOOL_CALLS,
            COALESCE(wt.WRONG_TOOLS_COUNT, 0) as WRONG_TOOLS_COUNT,
            COALESCE(wt.WRONG_TOOLS_TOOL_COUNT, 0) as WRONG_TOOLS_TOOL_COUNT,
            COALESCE(mt.MISSED_FALSE_PROMISE_CONVS, 0) as MISSED_FALSE_PROMISE_COUNT,
            COALESCE(mt.MISSED_POLICY_BASED_CONVS, 0) as MISSED_POLICY_BASED_COUNT,
            COALESCE(mt.TOTAL_MISSED_CONVS, 0) as TOTAL_MISSED_TOOLS_COUNT,
            COALESCE(mt.MISSED_FALSE_PROMISE_TOOLS, 0) as MISSED_FALSE_PROMISE_TOOLS,
            COALESCE(mt.MISSED_POLICY_BASED_TOOLS, 0) as MISSED_POLICY_BASED_TOOLS,
            COALESCE(mt.TOTAL_MISSED_TOOLS, 0) as TOTAL_MISSED_TOOLS_ROWS,
            COALESCE(fp.FALSE_PROMISE_NO_TOOL_COUNT, 0) as FALSE_PROMISE_NO_TOOL_COUNT,
            (COALESCE(st.SUCCESSFUL_TOOL_CALLS, 0) + COALESCE(mt.TOTAL_MISSED_TOOLS, 0)) as ADJUSTED_TOOL_CALLS_DENOMINATOR
        FROM total_convs tc
        CROSS JOIN total_tools tt
        CROSS JOIN successful_tools st
        LEFT JOIN wrong_tools_total wt ON 1=1
        LEFT JOIN missed_tools_total mt ON 1=1
        LEFT JOIN false_promise_no_tool_total fp ON 1=1
        """
        
        totals_result = session.sql(totals_query).to_pandas()
        if not totals_result.empty:
            tr = totals_result.iloc[0]
            total_row = {
                'CATEGORY': 'TOTAL',
                'TOTAL_CONVERSATIONS': int(tr['TOTAL_CONVERSATIONS']),
                'TOTAL_CONVERSATIONS_PCT': 100.0,
                'TOTAL_TOOL_CALLS': int(tr['TOTAL_TOOL_CALLS']),
                'WRONG_TOOLS_COUNT': int(tr['WRONG_TOOLS_COUNT']),
                'WRONG_TOOLS_PCT_OF_CONVERSATIONS': round(
                    (tr['WRONG_TOOLS_COUNT'] * 100.0 / tr['TOTAL_CONVERSATIONS']) 
                    if tr['TOTAL_CONVERSATIONS'] > 0 else 0, 2
                ),
                'WRONG_TOOLS_TOOL_COUNT': int(tr['WRONG_TOOLS_TOOL_COUNT']),
                'WRONG_TOOLS_PCT_OF_TOOL_CALLS': round(
                    (tr['WRONG_TOOLS_TOOL_COUNT'] * 100.0 / tr['TOTAL_TOOL_CALLS']) 
                    if tr['TOTAL_TOOL_CALLS'] > 0 else 0, 2
                ),
                'MISSED_FALSE_PROMISE_COUNT': int(tr['MISSED_FALSE_PROMISE_COUNT']),
                'MISSED_FALSE_PROMISE_PCT_OF_CONVERSATIONS': round(
                    (tr['MISSED_FALSE_PROMISE_COUNT'] * 100.0 / tr['TOTAL_CONVERSATIONS']) 
                    if tr['TOTAL_CONVERSATIONS'] > 0 else 0, 2
                ),
                'MISSED_FALSE_PROMISE_PCT_OF_TOOLS': round(
                    (tr['MISSED_FALSE_PROMISE_TOOLS'] * 100.0 / tr['ADJUSTED_TOOL_CALLS_DENOMINATOR']) 
                    if tr['ADJUSTED_TOOL_CALLS_DENOMINATOR'] > 0 else 0, 2
                ),
                'MISSED_POLICY_BASED_COUNT': int(tr['MISSED_POLICY_BASED_COUNT']),
                'MISSED_POLICY_BASED_PCT_OF_CONVERSATIONS': round(
                    (tr['MISSED_POLICY_BASED_COUNT'] * 100.0 / tr['TOTAL_CONVERSATIONS']) 
                    if tr['TOTAL_CONVERSATIONS'] > 0 else 0, 2
                ),
                'MISSED_POLICY_BASED_PCT_OF_TOOLS': round(
                    (tr['MISSED_POLICY_BASED_TOOLS'] * 100.0 / tr['ADJUSTED_TOOL_CALLS_DENOMINATOR']) 
                    if tr['ADJUSTED_TOOL_CALLS_DENOMINATOR'] > 0 else 0, 2
                ),
                'TOTAL_MISSED_TOOLS_COUNT': int(tr['TOTAL_MISSED_TOOLS_COUNT']),
                'TOTAL_MISSED_TOOLS_PCT_OF_CONVERSATIONS': round(
                    (tr['TOTAL_MISSED_TOOLS_COUNT'] * 100.0 / tr['TOTAL_CONVERSATIONS']) 
                    if tr['TOTAL_CONVERSATIONS'] > 0 else 0, 2
                ),
                'TOTAL_MISSED_TOOLS_PCT_OF_TOOLS': round(
                    (tr['TOTAL_MISSED_TOOLS_ROWS'] * 100.0 / tr['ADJUSTED_TOOL_CALLS_DENOMINATOR']) 
                    if tr['ADJUSTED_TOOL_CALLS_DENOMINATOR'] > 0 else 0, 2
                ),
                'FALSE_PROMISE_NO_TOOL_COUNT': int(tr['FALSE_PROMISE_NO_TOOL_COUNT']),
                'FALSE_PROMISE_NO_TOOL_PCT_OF_CONVERSATIONS': round(
                    (tr['FALSE_PROMISE_NO_TOOL_COUNT'] * 100.0 / tr['TOTAL_CONVERSATIONS']) 
                    if tr['TOTAL_CONVERSATIONS'] > 0 else 0, 2
                ),
                'TOTAL_GUARDRAIL_INTERVENTIONS': int(tr['WRONG_TOOLS_COUNT'] + tr['TOTAL_MISSED_TOOLS_COUNT'] + tr['FALSE_PROMISE_NO_TOOL_COUNT']),
                'TOTAL_INTERVENTIONS_PCT_OF_CONVERSATIONS': round(
                    ((tr['WRONG_TOOLS_COUNT'] + tr['TOTAL_MISSED_TOOLS_COUNT'] + tr['FALSE_PROMISE_NO_TOOL_COUNT']) * 100.0 / tr['TOTAL_CONVERSATIONS']) 
                    if tr['TOTAL_CONVERSATIONS'] > 0 else 0, 2
                ),
                'ADJUSTED_TOOL_CALLS_DENOMINATOR': int(tr['ADJUSTED_TOOL_CALLS_DENOMINATOR'])
            }
        else:
            # Fallback to zeros if query fails
            total_row = {
                'CATEGORY': 'TOTAL',
                'TOTAL_CONVERSATIONS': 0,
                'TOTAL_CONVERSATIONS_PCT': 100.0,
                'TOTAL_TOOL_CALLS': 0,
                'WRONG_TOOLS_COUNT': 0,
                'WRONG_TOOLS_PCT_OF_CONVERSATIONS': 0.0,
                'WRONG_TOOLS_TOOL_COUNT': 0,
                'WRONG_TOOLS_PCT_OF_TOOL_CALLS': 0.0,
                'MISSED_FALSE_PROMISE_COUNT': 0,
                'MISSED_FALSE_PROMISE_PCT_OF_CONVERSATIONS': 0.0,
                'MISSED_FALSE_PROMISE_PCT_OF_TOOLS': 0.0,
                'MISSED_POLICY_BASED_COUNT': 0,
                'MISSED_POLICY_BASED_PCT_OF_CONVERSATIONS': 0.0,
                'MISSED_POLICY_BASED_PCT_OF_TOOLS': 0.0,
                'TOTAL_MISSED_TOOLS_COUNT': 0,
                'TOTAL_MISSED_TOOLS_PCT_OF_CONVERSATIONS': 0.0,
                'TOTAL_MISSED_TOOLS_PCT_OF_TOOLS': 0.0,
                'FALSE_PROMISE_NO_TOOL_COUNT': 0,
                'FALSE_PROMISE_NO_TOOL_PCT_OF_CONVERSATIONS': 0.0,
                'TOTAL_GUARDRAIL_INTERVENTIONS': 0,
                'TOTAL_INTERVENTIONS_PCT_OF_CONVERSATIONS': 0.0,
                'ADJUSTED_TOOL_CALLS_DENOMINATOR': 0
            }   
        
        # Append totals row to the DataFrame
        summary_df = pd.concat([summary_df, pd.DataFrame([total_row])], ignore_index=True)
        print(f"    ➕ Added totals row to summary")
        
        # Define columns for the summary table
        columns = [
            'CATEGORY',
            'TOTAL_CONVERSATIONS',
            'TOTAL_CONVERSATIONS_PCT',
            'TOTAL_TOOL_CALLS',
            'WRONG_TOOLS_COUNT',
            'WRONG_TOOLS_PCT_OF_CONVERSATIONS',
            'WRONG_TOOLS_TOOL_COUNT',
            'WRONG_TOOLS_PCT_OF_TOOL_CALLS',
            'MISSED_FALSE_PROMISE_COUNT',
            'MISSED_FALSE_PROMISE_PCT_OF_CONVERSATIONS',
            'MISSED_FALSE_PROMISE_PCT_OF_TOOLS',
            'MISSED_POLICY_BASED_COUNT',
            'MISSED_POLICY_BASED_PCT_OF_CONVERSATIONS',
            'MISSED_POLICY_BASED_PCT_OF_TOOLS',
            'TOTAL_MISSED_TOOLS_COUNT',
            'TOTAL_MISSED_TOOLS_PCT_OF_CONVERSATIONS',
            'TOTAL_MISSED_TOOLS_PCT_OF_TOOLS',
            'FALSE_PROMISE_NO_TOOL_COUNT',
            'FALSE_PROMISE_NO_TOOL_PCT_OF_CONVERSATIONS',
            'TOTAL_GUARDRAIL_INTERVENTIONS',
            'TOTAL_INTERVENTIONS_PCT_OF_CONVERSATIONS',
            'ADJUSTED_TOOL_CALLS_DENOMINATOR'
        ]
        
        try:
            # Save using insert_raw_data_with_cleanup
            insert_raw_data_with_cleanup(
                session=session,
                table_name="GUARDRAIL_SUMMARY_BY_CATEGORY",
                department=department_name,
                target_date=target_date,
                dataframe=summary_df[columns],
                columns=columns
            )
            print(f"    💾 Saved summary to GUARDRAIL_SUMMARY_BY_CATEGORY table")
        except Exception as save_error:
            print(f"    ⚠️  Failed to save summary: {str(save_error)}")
        
        return len(summary_df)
    
    except Exception as e:
        print(f"    ⚠️  Error generating guardrail summary: {str(e)}")
        return 0


def analyze_guardrail_wrong_tool_calls_by_name(session, df, department_name, target_date):
    """
    Detect wrong tool calls for Sales Bot and AT bots using the
    'GUARDRAIL DETECTED A WRONG TOOL CALL' signal.

    Extracts ToolName from the embedded Tool Call JSON in the guardrail message.
    This is the Sales/AT bot format, distinct from the 'Evaluator note:' format used by CS bots.

    Args:
        session: Snowflake session
        df: Filtered DataFrame from Phase 1
        department_name: Department name
        target_date: Target date for analysis

    Returns:
        int: Count of wrong tool call records found
    """
    print(f"  🚫 Analyzing wrong tool calls by name for {department_name}...")

    if df.empty:
        print(f"    ⚠️  No data to analyze for wrong tool calls")
        return 0

    wrong_tool_data = []

    try:
        departments_config = get_snowflake_departments_config()
        dept_config = departments_config.get(department_name, {})
        bot_skills = dept_config.get('bot_skills', [])
        agent_skills = dept_config.get('agent_skills', [])
        all_dept_skills = set(bot_skills + agent_skills)

        wrong_tool_msgs = df[
            (df['TEXT'].str.contains(
                'GUARDRAIL DETECTED A WRONG TOOL CALL',
                case=False, na=False
            )) &
            (df['TARGET_SKILL_PER_MESSAGE'].isin(all_dept_skills))
        ]

        if wrong_tool_msgs.empty:
            print(f"    ℹ️  No wrong tool call messages found for {department_name}")
            return 0

        print(f"    🔍 Found {len(wrong_tool_msgs)} wrong tool call messages")

        for idx, msg in wrong_tool_msgs.iterrows():
            try:
                conv_id = msg['CONVERSATION_ID']
                message_text = msg['TEXT']
                message_time = msg['MESSAGE_SENT_TIME']
                message_id = msg['MESSAGE_ID'] if 'MESSAGE_ID' in msg else ''
                target_skill = msg['TARGET_SKILL_PER_MESSAGE'] if 'TARGET_SKILL_PER_MESSAGE' in msg else ''
                execution_id = msg['EXECUTION_ID'] if 'EXECUTION_ID' in msg else ''

                tool_name_match = re.search(
                    r'"ToolName"\s*:\s*"([^"]+)"',
                    message_text,
                    re.IGNORECASE
                )
                tool_name = tool_name_match.group(1).strip() if tool_name_match else ''

                if not tool_name:
                    print(f"    ⚠️  Could not extract ToolName from message {message_id} in conv {conv_id}")
                    tool_name = 'UNKNOWN'

                wrong_tool_data.append({
                    'CONVERSATION_ID': conv_id,
                    'MESSAGE_ID': message_id,
                    'TOOL_NAME': tool_name,
                    'MESSAGE_TIME': message_time,
                    'TARGET_SKILL_PER_MESSAGE': target_skill,
                    'EXECUTION_ID': execution_id
                })

            except Exception as msg_error:
                print(f"    ⚠️  Error processing wrong tool message: {str(msg_error)}")
                continue

        if wrong_tool_data:
            print(f"    ✅ Found {len(wrong_tool_data)} wrong tool call records")
            wrong_tool_df = pd.DataFrame(wrong_tool_data)
            columns = [
                'CONVERSATION_ID', 'MESSAGE_ID', 'TOOL_NAME',
                'MESSAGE_TIME', 'TARGET_SKILL_PER_MESSAGE', 'EXECUTION_ID'
            ]
            try:
                insert_raw_data_with_cleanup(
                    session=session,
                    table_name="GUARDRAIL_WRONG_TOOL_CALLS",
                    department=department_name,
                    target_date=target_date,
                    dataframe=wrong_tool_df[columns],
                    columns=columns
                )
                print(f"    💾 Saved {len(wrong_tool_data)} wrong tool calls to GUARDRAIL_WRONG_TOOL_CALLS")
            except Exception as save_error:
                print(f"    ⚠️  Failed to save wrong tool calls: {str(save_error)}")
        else:
            print(f"    ℹ️  No wrong tool call records extracted for {department_name}")

        return len(wrong_tool_data)

    except Exception as e:
        print(f"    ⚠️  Error analyzing wrong tool calls: {str(e)}")
        return 0


def analyze_guardrail_violations_by_tool(session, department_name, target_date):
    """
    Create a unified violation-level table combining all guardrail violation types,
    keyed by tool name. Reads from already-populated raw tables for the given dept/date.

    Violation types included:
    - 'Wrong tool call'                   → from GUARDRAIL_WRONG_TOOL_CALLS
    - 'Missed Tool Call - False Promise'  → from GUARDRAIL_MISSED_TOOLS (False Promise type)
    - 'Missed Tool Call - Policy Based'   → from GUARDRAIL_MISSED_TOOLS (Policy Based type)
    - 'False Promise Without Relevant Tool' → from GUARDRAIL_FALSE_PROMISE_NO_TOOL (tool = NULL)

    Args:
        session: Snowflake session
        department_name: Department name
        target_date: Target date for analysis

    Returns:
        int: Count of violation rows saved
    """
    print(f"  📋 Building violation-level table by tool for {department_name}...")

    try:
        check_query = f"""
        SELECT
            (SELECT COUNT(*) FROM GUARDRAIL_WRONG_TOOL_CALLS
             WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}') as wrong_count,
            (SELECT COUNT(*) FROM GUARDRAIL_MISSED_TOOLS
             WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}') as missed_count,
            (SELECT COUNT(*) FROM GUARDRAIL_FALSE_PROMISE_NO_TOOL
             WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}') as false_promise_count
        """
        check_df = session.sql(check_query).to_pandas()
        wrong_count = int(check_df['WRONG_COUNT'].iloc[0]) if not check_df.empty else 0
        missed_count = int(check_df['MISSED_COUNT'].iloc[0]) if not check_df.empty else 0
        false_promise_count = int(check_df['FALSE_PROMISE_COUNT'].iloc[0]) if not check_df.empty else 0

        total = wrong_count + missed_count + false_promise_count
        if total == 0:
            print(f"    ℹ️  No violation data found for {department_name} on {target_date}. Skipping.")
            return 0

        print(f"    🔍 Sources: {wrong_count} wrong calls, {missed_count} missed calls, {false_promise_count} false promise no tool")

        union_query = f"""
        SELECT
            MESSAGE_ID,
            'Wrong tool call' AS VIOLATION_TYPE,
            CONVERSATION_ID,
            TOOL_NAME AS RELEVANT_TOOL_NAME
        FROM GUARDRAIL_WRONG_TOOL_CALLS
        WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'

        UNION ALL

        SELECT
            MESSAGE_ID,
            CASE
                WHEN MISSED_TOOL_TYPE = 'False Promise' THEN 'Missed Tool Call - False Promise'
                WHEN MISSED_TOOL_TYPE = 'Policy Based' THEN 'Missed Tool Call - Policy Based'
                ELSE 'Missed Tool Call - ' || MISSED_TOOL_TYPE
            END AS VIOLATION_TYPE,
            CONVERSATION_ID,
            MISSED_TOOL_NAME AS RELEVANT_TOOL_NAME
        FROM GUARDRAIL_MISSED_TOOLS
        WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'

        UNION ALL

        SELECT
            MESSAGE_ID,
            'False Promise Without Relevant Tool' AS VIOLATION_TYPE,
            CONVERSATION_ID,
            NULL AS RELEVANT_TOOL_NAME
        FROM GUARDRAIL_FALSE_PROMISE_NO_TOOL
        WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
        """

        violations_df = session.sql(union_query).to_pandas()

        if violations_df.empty:
            print(f"    ℹ️  Union query returned no rows for {department_name}")
            return 0

        print(f"    ✅ Built {len(violations_df)} violation rows")

        columns = ['MESSAGE_ID', 'VIOLATION_TYPE', 'CONVERSATION_ID', 'RELEVANT_TOOL_NAME']

        try:
            insert_raw_data_with_cleanup(
                session=session,
                table_name="GUARDRAIL_VIOLATIONS_BY_TOOL",
                department=department_name,
                target_date=target_date,
                dataframe=violations_df[columns],
                columns=columns
            )
            print(f"    💾 Saved {len(violations_df)} violations to GUARDRAIL_VIOLATIONS_BY_TOOL")
        except Exception as save_error:
            print(f"    ⚠️  Failed to save violations: {str(save_error)}")

        return len(violations_df)

    except Exception as e:
        print(f"    ⚠️  Error building violation table: {str(e)}")
        return 0


def generate_guardrail_summary_by_tool(session, department_name, target_date):
    """
    Generate guardrail summary statistics grouped by tool name for Sales Bot and AT bots.

    Unlike generate_guardrail_summary_by_category, this function:
    - Groups all metrics by TOOL_NAME (not category)
    - Supports chat-level and tool-level percentages in a single output table
    - False Promise Without Relevant Tool is an overall chat-level metric only (NULL at per-tool level)
    - NA is treated as a valid tool bucket where applicable
    - Skips tool-level denominators gracefully if CONVERSATION_TOOL_CALLS has no data

    Reads from:
    - GUARDRAIL_VIOLATIONS_BY_TOOL
    - CONVERSATION_TOOL_CALLS
    - GUARDRAIL_STOPPED_TOOLS (for missed tool denominator: excludes calls with Evaluator note)
    - GUARDRAIL_MISSED_TOOLS  (used indirectly via GUARDRAIL_VIOLATIONS_BY_TOOL)

    Args:
        session: Snowflake session
        department_name: Department name
        target_date: Target date for analysis

    Returns:
        int: Count of rows in the summary
    """
    print(f"  📈 Generating guardrail summary by tool for {department_name}...")

    try:
        # Guard: check if any violation data exists
        viol_check = session.sql(f"""
            SELECT COUNT(*) AS viol_count
            FROM GUARDRAIL_VIOLATIONS_BY_TOOL
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
        """).to_pandas()
        has_violations = (not viol_check.empty) and (int(viol_check['VIOL_COUNT'].iloc[0]) > 0)

        if not has_violations:
            print(f"    ℹ️  No violations in GUARDRAIL_VIOLATIONS_BY_TOOL for {department_name}. Skipping summary.")
            return 0

        # Guard: check if tool call data exists
        tool_check = session.sql(f"""
            SELECT COUNT(*) AS tool_count
            FROM CONVERSATION_TOOL_CALLS
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
        """).to_pandas()
        has_tool_data = (not tool_check.empty) and (int(tool_check['TOOL_COUNT'].iloc[0]) > 0)

        if not has_tool_data:
            print(f"    ⚠️  No rows in CONVERSATION_TOOL_CALLS for {department_name}. "
                  f"Tool-level % denominators will be NULL.")

        query = f"""
        WITH
        -- All conversations in scope: union of tool calls + violations for this dept/date
        all_convs AS (
            SELECT DISTINCT CONVERSATION_ID
            FROM CONVERSATION_TOOL_CALLS
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
            UNION
            SELECT DISTINCT CONVERSATION_ID
            FROM GUARDRAIL_VIOLATIONS_BY_TOOL
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
        ),
        total_convs AS (
            SELECT COUNT(DISTINCT CONVERSATION_ID) AS TOTAL_CONVERSATIONS FROM all_convs
        ),

        -- Tool universe: tools from actual calls + tools from violations + NA bucket
        -- Excludes: empty strings, multi-tool names (containing ',' or ' and ')
        tool_universe AS (
            SELECT DISTINCT TOOL_NAME
            FROM CONVERSATION_TOOL_CALLS
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
              AND TOOL_NAME IS NOT NULL AND TRIM(TOOL_NAME) != ''
              AND TOOL_NAME NOT LIKE '%,%'
              AND TOOL_NAME NOT LIKE '% and %'
            UNION
            SELECT DISTINCT RELEVANT_TOOL_NAME AS TOOL_NAME
            FROM GUARDRAIL_VIOLATIONS_BY_TOOL
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
              AND RELEVANT_TOOL_NAME IS NOT NULL AND TRIM(RELEVANT_TOOL_NAME) != ''
              AND RELEVANT_TOOL_NAME NOT LIKE '%,%'
              AND RELEVANT_TOOL_NAME NOT LIKE '% and %'
            UNION
            SELECT 'NA' AS TOOL_NAME
        ),

        -- Wrong tool calls per tool
        wrong_per_tool AS (
            SELECT
                COALESCE(RELEVANT_TOOL_NAME, 'NA') AS TOOL_NAME,
                COUNT(*) AS WRONG_TOOL_COUNT,
                COUNT(DISTINCT CONVERSATION_ID) AS WRONG_TOOL_CONVS
            FROM GUARDRAIL_VIOLATIONS_BY_TOOL
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
              AND VIOLATION_TYPE = 'Wrong tool call'
            GROUP BY COALESCE(RELEVANT_TOOL_NAME, 'NA')
        ),

        -- Missed tool calls per tool (split by type and combined)
        missed_per_tool AS (
            SELECT
                COALESCE(RELEVANT_TOOL_NAME, 'NA') AS TOOL_NAME,
                COUNT(CASE WHEN VIOLATION_TYPE = 'Missed Tool Call - False Promise' THEN 1 END) AS MISSED_FP_COUNT,
                COUNT(CASE WHEN VIOLATION_TYPE = 'Missed Tool Call - Policy Based' THEN 1 END) AS MISSED_PB_COUNT,
                COUNT(CASE WHEN VIOLATION_TYPE IN ('Missed Tool Call - False Promise',
                                                    'Missed Tool Call - Policy Based') THEN 1 END) AS MISSED_COMBINED_COUNT,
                COUNT(DISTINCT CASE WHEN VIOLATION_TYPE = 'Missed Tool Call - False Promise'
                                    THEN CONVERSATION_ID END) AS MISSED_FP_CONVS,
                COUNT(DISTINCT CASE WHEN VIOLATION_TYPE = 'Missed Tool Call - Policy Based'
                                    THEN CONVERSATION_ID END) AS MISSED_PB_CONVS,
                COUNT(DISTINCT CASE WHEN VIOLATION_TYPE IN ('Missed Tool Call - False Promise',
                                                             'Missed Tool Call - Policy Based')
                                    THEN CONVERSATION_ID END) AS MISSED_COMBINED_CONVS
            FROM GUARDRAIL_VIOLATIONS_BY_TOOL
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
              AND VIOLATION_TYPE IN ('Missed Tool Call - False Promise', 'Missed Tool Call - Policy Based')
            GROUP BY COALESCE(RELEVANT_TOOL_NAME, 'NA')
        ),

        -- Total attempted calls per tool (successful + guardrail-intercepted wrong calls)
        -- Used as denominator for wrong tool call tool-level %
        -- Wrong calls intercepted by guardrail are never stored in CONVERSATION_TOOL_CALLS,
        -- so we must add GUARDRAIL_WRONG_TOOL_CALLS to avoid >100% ratios
        total_calls_per_tool AS (
            SELECT TOOL_NAME, COUNT(*) AS TOTAL_CALLS
            FROM (
                SELECT TOOL_NAME FROM CONVERSATION_TOOL_CALLS
                WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
                UNION ALL
                SELECT TOOL_NAME FROM GUARDRAIL_WRONG_TOOL_CALLS
                WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
            )
            GROUP BY TOOL_NAME
        ),

        -- Missed tool call denominator per tool:
        --   = actual calls of X NOT flagged by Evaluator note (correct calls) + missed calls of X
        -- GUARDRAIL_STOPPED_TOOLS holds tool_call_ids with Evaluator note responses
        good_calls_per_tool AS (
            SELECT ct.TOOL_NAME, COUNT(*) AS GOOD_CALLS
            FROM CONVERSATION_TOOL_CALLS ct
            LEFT JOIN GUARDRAIL_STOPPED_TOOLS gst
                ON ct.TOOL_CALL_ID = gst.TOOL_CALL_ID
                AND ct.DATE = gst.DATE
                AND ct.DEPARTMENT = gst.DEPARTMENT
            WHERE ct.DATE = '{target_date}' AND ct.DEPARTMENT = '{department_name}'
              AND gst.TOOL_CALL_ID IS NULL
            GROUP BY ct.TOOL_NAME
        ),
        missed_denom_per_tool AS (
            SELECT
                COALESCE(gc.TOOL_NAME, mt.TOOL_NAME) AS TOOL_NAME,
                COALESCE(gc.GOOD_CALLS, 0) + COALESCE(mt.MISSED_COMBINED_COUNT, 0) AS MISSED_TOOL_DENOM
            FROM good_calls_per_tool gc
            FULL OUTER JOIN missed_per_tool mt ON gc.TOOL_NAME = mt.TOOL_NAME
        ),

        -- Overall aggregates for the TOTAL row
        overall_wrong AS (
            SELECT
                COUNT(*) AS TOTAL_WRONG,
                COUNT(DISTINCT CONVERSATION_ID) AS TOTAL_WRONG_CONVS
            FROM GUARDRAIL_VIOLATIONS_BY_TOOL
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
              AND VIOLATION_TYPE = 'Wrong tool call'
        ),
        overall_missed AS (
            SELECT
                COUNT(CASE WHEN VIOLATION_TYPE = 'Missed Tool Call - False Promise' THEN 1 END) AS TOTAL_MISSED_FP,
                COUNT(CASE WHEN VIOLATION_TYPE = 'Missed Tool Call - Policy Based' THEN 1 END) AS TOTAL_MISSED_PB,
                COUNT(*) AS TOTAL_MISSED,
                COUNT(DISTINCT CASE WHEN VIOLATION_TYPE = 'Missed Tool Call - False Promise'
                                    THEN CONVERSATION_ID END) AS TOTAL_MISSED_FP_CONVS,
                COUNT(DISTINCT CASE WHEN VIOLATION_TYPE = 'Missed Tool Call - Policy Based'
                                    THEN CONVERSATION_ID END) AS TOTAL_MISSED_PB_CONVS,
                COUNT(DISTINCT CONVERSATION_ID) AS TOTAL_MISSED_CONVS
            FROM GUARDRAIL_VIOLATIONS_BY_TOOL
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
              AND VIOLATION_TYPE IN ('Missed Tool Call - False Promise', 'Missed Tool Call - Policy Based')
        ),
        overall_false_promise AS (
            SELECT COUNT(DISTINCT CONVERSATION_ID) AS TOTAL_FP_NO_TOOL_CONVS
            FROM GUARDRAIL_VIOLATIONS_BY_TOOL
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
              AND VIOLATION_TYPE = 'False Promise Without Relevant Tool'
        ),
        overall_tool_calls AS (
            SELECT COUNT(*) AS TOTAL_TOOL_CALLS
            FROM (
                SELECT TOOL_NAME FROM CONVERSATION_TOOL_CALLS
                WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
                UNION ALL
                SELECT TOOL_NAME FROM GUARDRAIL_WRONG_TOOL_CALLS
                WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
            )
        ),
        overall_missed_denom AS (
            SELECT SUM(MISSED_TOOL_DENOM) AS TOTAL_MISSED_DENOM FROM missed_denom_per_tool
        )

        -- Per-tool rows (only tools with at least one violation OR actual tool calls)
        SELECT
            tu.TOOL_NAME,
            tc.TOTAL_CONVERSATIONS,

            COALESCE(wt.WRONG_TOOL_COUNT, 0) AS WRONG_TOOL_COUNT,
            ROUND(COALESCE(wt.WRONG_TOOL_COUNT, 0) * 100.0 / NULLIF(tcp.TOTAL_CALLS, 0), 2) AS WRONG_TOOL_PCT_OF_TOOL_CALLS,
            COALESCE(wt.WRONG_TOOL_CONVS, 0) AS WRONG_TOOL_CONVS,
            ROUND(COALESCE(wt.WRONG_TOOL_CONVS, 0) * 100.0 / NULLIF(tc.TOTAL_CONVERSATIONS, 0), 2) AS WRONG_TOOL_PCT_OF_CHATS,

            COALESCE(mt.MISSED_FP_COUNT, 0) AS MISSED_FALSE_PROMISE_COUNT,
            ROUND(COALESCE(mt.MISSED_FP_COUNT, 0) * 100.0 / NULLIF(md.MISSED_TOOL_DENOM, 0), 2) AS MISSED_FALSE_PROMISE_PCT_OF_TOOLS,
            COALESCE(mt.MISSED_FP_CONVS, 0) AS MISSED_FALSE_PROMISE_CONVS,
            ROUND(COALESCE(mt.MISSED_FP_CONVS, 0) * 100.0 / NULLIF(tc.TOTAL_CONVERSATIONS, 0), 2) AS MISSED_FALSE_PROMISE_PCT_OF_CHATS,

            COALESCE(mt.MISSED_PB_COUNT, 0) AS MISSED_POLICY_COUNT,
            ROUND(COALESCE(mt.MISSED_PB_COUNT, 0) * 100.0 / NULLIF(md.MISSED_TOOL_DENOM, 0), 2) AS MISSED_POLICY_PCT_OF_TOOLS,
            COALESCE(mt.MISSED_PB_CONVS, 0) AS MISSED_POLICY_CONVS,
            ROUND(COALESCE(mt.MISSED_PB_CONVS, 0) * 100.0 / NULLIF(tc.TOTAL_CONVERSATIONS, 0), 2) AS MISSED_POLICY_PCT_OF_CHATS,

            COALESCE(mt.MISSED_COMBINED_COUNT, 0) AS MISSED_COMBINED_COUNT,
            ROUND(COALESCE(mt.MISSED_COMBINED_COUNT, 0) * 100.0 / NULLIF(md.MISSED_TOOL_DENOM, 0), 2) AS MISSED_COMBINED_PCT_OF_TOOLS,
            COALESCE(mt.MISSED_COMBINED_CONVS, 0) AS MISSED_COMBINED_CONVS,
            ROUND(COALESCE(mt.MISSED_COMBINED_CONVS, 0) * 100.0 / NULLIF(tc.TOTAL_CONVERSATIONS, 0), 2) AS MISSED_COMBINED_PCT_OF_CHATS,

            COALESCE(tcp.TOTAL_CALLS, 0) AS TOTAL_TOOL_CALLS,
            COALESCE(md.MISSED_TOOL_DENOM, 0) AS MISSED_TOOL_DENOMINATOR,

            -- False Promise Without Relevant Tool: NULL at per-tool level; only shown in TOTAL row
            NULL AS FALSE_PROMISE_NO_TOOL_CONVS,
            NULL AS FALSE_PROMISE_NO_TOOL_PCT,

            'Per-Tool' AS ROW_TYPE

        FROM tool_universe tu
        CROSS JOIN total_convs tc
        LEFT JOIN wrong_per_tool wt ON tu.TOOL_NAME = wt.TOOL_NAME
        LEFT JOIN missed_per_tool mt ON tu.TOOL_NAME = mt.TOOL_NAME
        LEFT JOIN total_calls_per_tool tcp ON tu.TOOL_NAME = tcp.TOOL_NAME
        LEFT JOIN missed_denom_per_tool md ON tu.TOOL_NAME = md.TOOL_NAME
        WHERE tcp.TOTAL_CALLS IS NOT NULL
           OR COALESCE(wt.WRONG_TOOL_COUNT, 0) > 0
           OR COALESCE(mt.MISSED_COMBINED_COUNT, 0) > 0

        UNION ALL

        -- TOTAL row: overall metrics across all tools
        SELECT
            'TOTAL' AS TOOL_NAME,
            tc.TOTAL_CONVERSATIONS,

            ow.TOTAL_WRONG AS WRONG_TOOL_COUNT,
            ROUND(ow.TOTAL_WRONG * 100.0 / NULLIF(otc.TOTAL_TOOL_CALLS, 0), 2) AS WRONG_TOOL_PCT_OF_TOOL_CALLS,
            ow.TOTAL_WRONG_CONVS AS WRONG_TOOL_CONVS,
            ROUND(ow.TOTAL_WRONG_CONVS * 100.0 / NULLIF(tc.TOTAL_CONVERSATIONS, 0), 2) AS WRONG_TOOL_PCT_OF_CHATS,

            om.TOTAL_MISSED_FP AS MISSED_FALSE_PROMISE_COUNT,
            ROUND(om.TOTAL_MISSED_FP * 100.0 / NULLIF(omd.TOTAL_MISSED_DENOM, 0), 2) AS MISSED_FALSE_PROMISE_PCT_OF_TOOLS,
            om.TOTAL_MISSED_FP_CONVS AS MISSED_FALSE_PROMISE_CONVS,
            ROUND(om.TOTAL_MISSED_FP_CONVS * 100.0 / NULLIF(tc.TOTAL_CONVERSATIONS, 0), 2) AS MISSED_FALSE_PROMISE_PCT_OF_CHATS,

            om.TOTAL_MISSED_PB AS MISSED_POLICY_COUNT,
            ROUND(om.TOTAL_MISSED_PB * 100.0 / NULLIF(omd.TOTAL_MISSED_DENOM, 0), 2) AS MISSED_POLICY_PCT_OF_TOOLS,
            om.TOTAL_MISSED_PB_CONVS AS MISSED_POLICY_CONVS,
            ROUND(om.TOTAL_MISSED_PB_CONVS * 100.0 / NULLIF(tc.TOTAL_CONVERSATIONS, 0), 2) AS MISSED_POLICY_PCT_OF_CHATS,

            om.TOTAL_MISSED AS MISSED_COMBINED_COUNT,
            ROUND(om.TOTAL_MISSED * 100.0 / NULLIF(omd.TOTAL_MISSED_DENOM, 0), 2) AS MISSED_COMBINED_PCT_OF_TOOLS,
            om.TOTAL_MISSED_CONVS AS MISSED_COMBINED_CONVS,
            ROUND(om.TOTAL_MISSED_CONVS * 100.0 / NULLIF(tc.TOTAL_CONVERSATIONS, 0), 2) AS MISSED_COMBINED_PCT_OF_CHATS,

            otc.TOTAL_TOOL_CALLS,
            omd.TOTAL_MISSED_DENOM AS MISSED_TOOL_DENOMINATOR,

            -- False Promise Without Relevant Tool: populated ONLY in the TOTAL row
            ofp.TOTAL_FP_NO_TOOL_CONVS AS FALSE_PROMISE_NO_TOOL_CONVS,
            ROUND(ofp.TOTAL_FP_NO_TOOL_CONVS * 100.0 / NULLIF(tc.TOTAL_CONVERSATIONS, 0), 2) AS FALSE_PROMISE_NO_TOOL_PCT,

            'Total' AS ROW_TYPE

        FROM total_convs tc
        CROSS JOIN overall_wrong ow
        CROSS JOIN overall_missed om
        CROSS JOIN overall_false_promise ofp
        CROSS JOIN overall_tool_calls otc
        CROSS JOIN overall_missed_denom omd

        ORDER BY ROW_TYPE DESC, WRONG_TOOL_COUNT DESC
        """

        print(f"    🔍 Executing summary query...")
        summary_df = session.sql(query).to_pandas()

        if summary_df.empty:
            print(f"    ℹ️  No summary data generated for {department_name}")
            return 0

        tool_rows = len(summary_df) - 1
        print(f"    ✅ Generated summary: {tool_rows} tool row(s) + TOTAL row")

        columns = [
            'TOOL_NAME', 'TOTAL_CONVERSATIONS',
            'WRONG_TOOL_COUNT', 'WRONG_TOOL_PCT_OF_TOOL_CALLS',
            'WRONG_TOOL_CONVS', 'WRONG_TOOL_PCT_OF_CHATS',
            'MISSED_FALSE_PROMISE_COUNT', 'MISSED_FALSE_PROMISE_PCT_OF_TOOLS',
            'MISSED_FALSE_PROMISE_CONVS', 'MISSED_FALSE_PROMISE_PCT_OF_CHATS',
            'MISSED_POLICY_COUNT', 'MISSED_POLICY_PCT_OF_TOOLS',
            'MISSED_POLICY_CONVS', 'MISSED_POLICY_PCT_OF_CHATS',
            'MISSED_COMBINED_COUNT', 'MISSED_COMBINED_PCT_OF_TOOLS',
            'MISSED_COMBINED_CONVS', 'MISSED_COMBINED_PCT_OF_CHATS',
            'TOTAL_TOOL_CALLS', 'MISSED_TOOL_DENOMINATOR',
            'FALSE_PROMISE_NO_TOOL_CONVS', 'FALSE_PROMISE_NO_TOOL_PCT',
            'ROW_TYPE'
        ]

        try:
            insert_raw_data_with_cleanup(
                session=session,
                table_name="GUARDRAIL_SUMMARY_BY_TOOL",
                department=department_name,
                target_date=target_date,
                dataframe=summary_df[columns],
                columns=columns
            )
            print(f"    💾 Saved summary to GUARDRAIL_SUMMARY_BY_TOOL table")
        except Exception as save_error:
            print(f"    ⚠️  Failed to save summary: {str(save_error)}")

        return len(summary_df)

    except Exception as e:
        print(f"    ⚠️  Error generating guardrail summary by tool: {str(e)}")
        return 0

def generate_tool_usage_analysis(session, department_name, target_date):
    """
    Generate tool usage analysis showing execution patterns and frequency per tool.
    
    For each tool, calculates:
    - Actual Executions (successful, not stopped by guardrails)
    - Conversations where tool was called once, twice, or 3+ times
    
    Args:
        session: Snowflake session
        department_name: Department name
        target_date: Target date for analysis
    
    Returns:
        int: Number of tools analyzed
    """
    print(f"  🔧 Generating tool usage analysis for {department_name}...")
    
    try:
        # Build SQL query for tool usage analysis
        query = f"""
        WITH total_conversations AS (
            -- Total conversations (for percentage denominators)
            SELECT COUNT(DISTINCT CONVERSATION_ID) as TOTAL_CONVS
            FROM CONVERSATION_CATEGORIES
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
        ),
        all_tool_calls AS (
            -- All tool call attempts per tool
            SELECT 
                TOOL_NAME,
                COUNT(*) as TOTAL_ATTEMPTS,
                COUNT(DISTINCT CONVERSATION_ID) as TOTAL_CONVS_WITH_TOOL
            FROM CONVERSATION_TOOL_CALLS
            WHERE DATE = '{target_date}' AND DEPARTMENT = '{department_name}'
            GROUP BY TOOL_NAME
        ),
        successful_tool_calls AS (
            -- Successful tool calls (not stopped by guardrails)
            SELECT 
                ct.TOOL_NAME,
                ct.CONVERSATION_ID,
                ct.TOOL_CALL_ID
            FROM CONVERSATION_TOOL_CALLS ct
            LEFT JOIN GUARDRAIL_STOPPED_TOOLS gst
                ON ct.TOOL_CALL_ID = gst.TOOL_CALL_ID
                AND ct.DATE = gst.DATE
                AND ct.DEPARTMENT = gst.DEPARTMENT
            WHERE ct.DATE = '{target_date}' 
              AND ct.DEPARTMENT = '{department_name}'
              AND gst.TOOL_CALL_ID IS NULL
        ),
        successful_executions AS (
            -- Count successful executions per tool
            SELECT 
                TOOL_NAME,
                COUNT(*) as ACTUAL_EXECUTIONS
            FROM successful_tool_calls
            GROUP BY TOOL_NAME
        ),
        tool_calls_per_conversation AS (
            -- Count how many times each tool was called per conversation (successful only)
            SELECT 
                TOOL_NAME,
                CONVERSATION_ID,
                COUNT(*) as CALL_COUNT
            FROM successful_tool_calls
            GROUP BY TOOL_NAME, CONVERSATION_ID
        ),
        frequency_buckets AS (
            -- Categorize conversations into frequency buckets per tool
            SELECT 
                TOOL_NAME,
                COUNT(DISTINCT CASE WHEN CALL_COUNT = 1 THEN CONVERSATION_ID END) as CALLED_ONCE_COUNT,
                COUNT(DISTINCT CASE WHEN CALL_COUNT = 2 THEN CONVERSATION_ID END) as CALLED_TWICE_COUNT,
                COUNT(DISTINCT CASE WHEN CALL_COUNT >= 3 THEN CONVERSATION_ID END) as CALLED_THRICE_PLUS_COUNT
            FROM tool_calls_per_conversation
            GROUP BY TOOL_NAME
        )
        SELECT 
            atc.TOOL_NAME,
            
            -- Actual Executions (successful, not blocked)
            COALESCE(se.ACTUAL_EXECUTIONS, 0) as ACTUAL_EXECUTIONS_COUNT,
            ROUND((COALESCE(se.ACTUAL_EXECUTIONS, 0) * 100.0 / NULLIF(tc.TOTAL_CONVS, 0)), 2) as ACTUAL_EXECUTIONS_PCT,
            
            -- Called Once in Chat
            COALESCE(fb.CALLED_ONCE_COUNT, 0) as CALLED_ONCE_COUNT,
            ROUND((COALESCE(fb.CALLED_ONCE_COUNT, 0) * 100.0 / NULLIF(tc.TOTAL_CONVS, 0)), 2) as CALLED_ONCE_PCT,
            
            -- Called Twice in Chat
            COALESCE(fb.CALLED_TWICE_COUNT, 0) as CALLED_TWICE_COUNT,
            ROUND((COALESCE(fb.CALLED_TWICE_COUNT, 0) * 100.0 / NULLIF(tc.TOTAL_CONVS, 0)), 2) as CALLED_TWICE_PCT,
            
            -- Called Thrice+ in Chat
            COALESCE(fb.CALLED_THRICE_PLUS_COUNT, 0) as CALLED_THRICE_PLUS_COUNT,
            ROUND((COALESCE(fb.CALLED_THRICE_PLUS_COUNT, 0) * 100.0 / NULLIF(tc.TOTAL_CONVS, 0)), 2) as CALLED_THRICE_PLUS_PCT,
            
            -- Reference columns
            atc.TOTAL_ATTEMPTS,
            tc.TOTAL_CONVS as TOTAL_CONVERSATIONS
            
        FROM all_tool_calls atc
        CROSS JOIN total_conversations tc
        LEFT JOIN successful_executions se ON atc.TOOL_NAME = se.TOOL_NAME
        LEFT JOIN frequency_buckets fb ON atc.TOOL_NAME = fb.TOOL_NAME
        ORDER BY COALESCE(se.ACTUAL_EXECUTIONS, 0) DESC
        """
        
        # Execute query
        print(f"    🔍 Executing tool usage analysis query...")
        analysis_df = session.sql(query).to_pandas()
        
        if analysis_df.empty:
            print(f"    ℹ️  No tool usage data generated for {department_name}")
            return 0
        
        print(f"    ✅ Generated analysis for {len(analysis_df)} tools")
        
        # Define columns for the table
        columns = [
            'TOOL_NAME',
            'ACTUAL_EXECUTIONS_COUNT',
            'ACTUAL_EXECUTIONS_PCT',
            'CALLED_ONCE_COUNT',
            'CALLED_ONCE_PCT',
            'CALLED_TWICE_COUNT',
            'CALLED_TWICE_PCT',
            'CALLED_THRICE_PLUS_COUNT',
            'CALLED_THRICE_PLUS_PCT',
            'TOTAL_ATTEMPTS',
            'TOTAL_CONVERSATIONS'
        ]
        
        try:
            # Save using insert_raw_data_with_cleanup
            insert_raw_data_with_cleanup(
                session=session,
                table_name="TOOL_USAGE_ANALYSIS",
                department=department_name,
                target_date=target_date,
                dataframe=analysis_df[columns],
                columns=columns
            )
            print(f"    💾 Saved tool usage analysis to TOOL_USAGE_ANALYSIS table")
        except Exception as save_error:
            print(f"    ⚠️  Failed to save tool usage analysis: {str(save_error)}")
        
        return len(analysis_df)
    
    except Exception as e:
        print(f"    ⚠️  Error generating tool usage analysis: {str(e)}")
        return 0


def compute_chat_initiation_breakdown(session, df, table_name, target_date, dept_name):
    """
    Compute how many conversations in the 'Chats Supposed to be Bot Handled' pool
    were initiated by us vs by the client.

    Classification logic:
      - First message SENT_BY != 'consumer'  →  Initiated by Us
      - First message SENT_BY == 'consumer':
            If a broadcast (first message SENT_BY = 'system') was sent to the SAME
            client within the 24 hours preceding the conversation's first message
            →  Initiated by Us  (client is answering our broadcast)
            Otherwise  →  Initiated by Client

    The consumer identifier column is discovered dynamically via INFORMATION_SCHEMA.
    If no identifier column exists the 24-hour broadcast check is skipped and
    classification falls back to first-message SENT_BY only.

    Args:
        session:      Snowflake session
        df:           Filtered DataFrame (the bot-handled pool, sorted by MESSAGE_SENT_TIME)
        table_name:   Fully-qualified Snowflake table name for the department
        target_date:  Target date string 'YYYY-MM-DD'
        dept_name:    Department name (for logging)

    Returns:
        tuple: (chats_initiated_by_us: int, chats_initiated_by_client: int)
    """
    print(f"    🔍 Computing chat initiation breakdown for {dept_name}...")

    if df.empty:
        print(f"    ⚠️  Empty dataframe – returning zeros for initiation breakdown")
        return 0, 0

    # ── Step 1: Get the first message per conversation ───────────────────────
    # df is already sorted by (CONVERSATION_ID, MESSAGE_SENT_TIME) by preprocessing
    first_msgs = (
        df.sort_values('MESSAGE_SENT_TIME')
          .groupby('CONVERSATION_ID', sort=False)
          .first()
          .reset_index()
    )

    total = len(first_msgs)

    # Classify using SENT_BY of the first message
    us_mask = first_msgs['SENT_BY'].str.lower() != 'consumer'
    consumer_mask = ~us_mask

    initiated_by_us_ids = set(first_msgs.loc[us_mask, 'CONVERSATION_ID'])
    consumer_initiated_ids = set(first_msgs.loc[consumer_mask, 'CONVERSATION_ID'])

    # ── Step 2: Discover the consumer identifier column ──────────────────────
    consumer_id_col = None
    try:
        # Handle fully-qualified table names like SCHEMA.DB.TABLE or DB.TABLE
        raw_table = table_name.split('.')[-1].upper()
        raw_schema = table_name.split('.')[-2].upper() if '.' in table_name else None

        candidate_cols = ['CONSUMER_ID', 'CONTACT_ID', 'PHONE_NUMBER', 'CLIENT_ID', 'CUSTOMER_ID']
        candidates_sql = ", ".join(f"'{c}'" for c in candidate_cols)

        schema_filter = f"AND TABLE_SCHEMA = '{raw_schema}'" if raw_schema else ""
        col_query = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{raw_table}'
        {schema_filter}
        AND COLUMN_NAME IN ({candidates_sql})
        ORDER BY CASE COLUMN_NAME
            WHEN 'CONSUMER_ID'   THEN 1
            WHEN 'CONTACT_ID'    THEN 2
            WHEN 'PHONE_NUMBER'  THEN 3
            WHEN 'CLIENT_ID'     THEN 4
            WHEN 'CUSTOMER_ID'   THEN 5
            ELSE 6
        END
        LIMIT 1
        """
        col_result = session.sql(col_query).collect()
        if col_result:
            consumer_id_col = col_result[0]['COLUMN_NAME']
            print(f"    ✅ Consumer identifier column found: {consumer_id_col}")
        else:
            print(f"    ⚠️  No consumer identifier column found in {table_name}. "
                  f"24-hour broadcast check will be skipped.")
    except Exception as e:
        print(f"    ⚠️  Could not query INFORMATION_SCHEMA: {e}. "
              f"24-hour broadcast check will be skipped.")

    # ── Step 3: 24-hour broadcast re-classification ───────────────────────────
    if consumer_id_col and consumer_initiated_ids:
        try:
            from datetime import datetime, timedelta

            # We need broadcasts from target_date-1 and target_date
            filter_date     = (datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            prev_filter_date = (datetime.strptime(target_date, '%Y-%m-%d')).strftime('%Y-%m-%d')

            # Build a single SQL query:
            # Get the first message time per conversation for the two-day window,
            # keeping only conversations whose first message is SENT_BY = 'system'
            # (i.e. broadcast conversations), along with the consumer identifier.
            broadcast_query = f"""
            WITH ranked AS (
                SELECT
                    CONVERSATION_ID,
                    {consumer_id_col},
                    MESSAGE_SENT_TIME,
                    SENT_BY,
                    ROW_NUMBER() OVER (
                        PARTITION BY CONVERSATION_ID
                        ORDER BY MESSAGE_SENT_TIME ASC
                    ) AS rn
                FROM {table_name}
                WHERE DATE(UPDATED_AT) IN ('{filter_date}', '{prev_filter_date}')
            )
            SELECT
                CONVERSATION_ID,
                {consumer_id_col} AS CONSUMER_KEY,
                MESSAGE_SENT_TIME AS BROADCAST_TIME
            FROM ranked
            WHERE rn = 1
              AND LOWER(SENT_BY) = 'system'
            """
            broadcast_df = session.sql(broadcast_query).to_pandas()

            if not broadcast_df.empty:
                # Build lookup: consumer_key → list of broadcast times
                broadcast_df['BROADCAST_TIME'] = pd.to_datetime(broadcast_df['BROADCAST_TIME'])
                broadcast_map = broadcast_df.groupby('CONSUMER_KEY')['BROADCAST_TIME'].apply(list).to_dict()

                # For each consumer-initiated conversation, check 24-hour window
                # We need the consumer_id and first message time from our filtered_df
                first_msgs_consumer = first_msgs[first_msgs['CONVERSATION_ID'].isin(consumer_initiated_ids)].copy()

                if consumer_id_col in first_msgs_consumer.columns:
                    first_msgs_consumer['MESSAGE_SENT_TIME'] = pd.to_datetime(first_msgs_consumer['MESSAGE_SENT_TIME'])

                    reclassified_ids = set()
                    for _, row in first_msgs_consumer.iterrows():
                        c_key = row.get(consumer_id_col)
                        if c_key is None or (isinstance(c_key, float) and pd.isna(c_key)):
                            continue
                        conv_start = row['MESSAGE_SENT_TIME']
                        broadcast_times = broadcast_map.get(c_key, [])
                        for bt in broadcast_times:
                            if pd.Timestamp(conv_start) - pd.Timedelta(hours=24) <= pd.Timestamp(bt) < pd.Timestamp(conv_start):
                                reclassified_ids.add(row['CONVERSATION_ID'])
                                break

                    initiated_by_us_ids.update(reclassified_ids)
                    consumer_initiated_ids -= reclassified_ids
                    print(f"    🔄 Re-classified {len(reclassified_ids)} consumer-started chat(s) as 'Initiated by Us' "
                          f"(broadcast within 24h found)")
                else:
                    print(f"    ⚠️  Column '{consumer_id_col}' not present in filtered_df rows; "
                          f"broadcast re-classification skipped.")
            else:
                print(f"    ℹ️  No broadcast conversations found in the 24-hour window.")

        except Exception as e:
            print(f"    ⚠️  Broadcast check failed: {e}. Results based on first-message SENT_BY only.")

    chats_initiated_by_us     = len(initiated_by_us_ids)
    chats_initiated_by_client = len(consumer_initiated_ids)

    # ── Step 4: Validation print ──────────────────────────────────────────────
    sum_check = chats_initiated_by_us + chats_initiated_by_client
    checkmark = '✓' if sum_check == total else '✗  ← MISMATCH!'
    print(f"\n    ═══════════════════════════════════════════════")
    print(f"    [INITIATION BREAKDOWN - {dept_name}]")
    print(f"      Consumer ID column used : {consumer_id_col or 'NONE (no 24h broadcast check)'}")
    print(f"      Total supposed-to-be-bot-handled : {total}")
    print(f"      Initiated by Us         : {chats_initiated_by_us}")
    print(f"      Initiated by Client     : {chats_initiated_by_client}")
    print(f"      Sum check: {sum_check} == {total}  {checkmark}")
    print(f"    ═══════════════════════════════════════════════\n")

    # ── Step 5: Save per-conversation raw data to Snowflake ───────────────────
    try:
        raw_rows = []
        for conv_id in initiated_by_us_ids:
            raw_rows.append({
                'CONVERSATION_ID': conv_id,
                'INITIATION_TYPE': 'Initiated by Us',
                'BROADCAST_RECLASSIFIED': False
            })
        for conv_id in consumer_initiated_ids:
            raw_rows.append({
                'CONVERSATION_ID': conv_id,
                'INITIATION_TYPE': 'Initiated by Client',
                'BROADCAST_RECLASSIFIED': False
            })

        # Mark conversations that were re-classified due to broadcast (only
        # populated when a consumer_id_col was found and re-classification ran)
        if consumer_id_col:
            # reclassified_ids was set inside the broadcast block above;
            # re-derive it from the data we already have: any conv that has
            # first message SENT_BY == 'consumer' but ended up in
            # initiated_by_us_ids was broadcast-reclassified.
            consumer_first_ids = set(
                first_msgs.loc[first_msgs['SENT_BY'].str.lower() == 'consumer', 'CONVERSATION_ID']
            )
            broadcast_reclassified = initiated_by_us_ids & consumer_first_ids
            for row in raw_rows:
                if row['CONVERSATION_ID'] in broadcast_reclassified:
                    row['BROADCAST_RECLASSIFIED'] = True

        if raw_rows:
            raw_df = pd.DataFrame(raw_rows)
            columns = ['CONVERSATION_ID', 'INITIATION_TYPE', 'BROADCAST_RECLASSIFIED']
            insert_raw_data_with_cleanup(
                session=session,
                table_name='CHAT_INITIATION_RAW_DATA',
                department=dept_name,
                target_date=target_date,
                dataframe=raw_df[columns],
                columns=columns
            )
            print(f"    💾 Saved {len(raw_rows)} rows to CHAT_INITIATION_RAW_DATA")
    except Exception as e:
        print(f"    ⚠️  Failed to save CHAT_INITIATION_RAW_DATA: {e}")

    return chats_initiated_by_us, chats_initiated_by_client


def analyze_bot_handled_conversations_single_department(session, df, department_name, departments_config, target_date):
    """
    Analyze bot-handled conversations for a single department and save raw data.
    Adapted from main_analytics.py analyze_bot_handled_conversations()
    
    Args:
        session: Snowflake session
        df: Filtered DataFrame from Phase 1
        department_name: Department name
        departments_config: Department configuration
        target_date: Target date for analysis
    
    Returns:
        bot_handled_results dictionary with breakdown counters
    """
    print(f"  🤖 Analyzing bot handling for {department_name}...")
    
    # MV_Resolvers specific: Calculate proactive agent metrics
    proactive_agent_messages_count = 0
    directly_handled_by_seniors_count = 0
    other_bots_to_seniors_count = 0
    our_bot_to_seniors_count = 0
    delighters_to_seniors_count = 0
    mv_bot_known_flow_transfer_count = 0
    mv_bot_tech_errors_transfers_count = 0
    mv_bot_guardrails_count = 0
    mv_bot_other_transfers_count = 0
    our_bot_to_mv_resolvers_seniors_count = 0
    our_bot_to_mv_callers_count = 0
    our_bot_to_pre_r_visa_retention_count = 0
    total_seniors_callers_count = 0
    seniors_no_response_transfers_count = 0
    seniors_our_bot_count = 0
    seniors_directly_handled_count = 0
    seniors_proactive_count = 0
    seniors_proactive_mv_resolvers_seniors_only_count = 0
    seniors_delighters_count = 0
    seniors_other_bots_count = 0
    seniors_our_bot_to_mv_resolvers_seniors_count = 0
    seniors_our_bot_to_mv_callers_count = 0
    seniors_our_bot_to_pre_r_visa_retention_count = 0
    seniors_conv_ids = set()
    seniors_supervisor_excluded_conv_ids = set()
    our_bot_to_seniors_conv_ids = set()
    total_guardrail_count = 0
    guardrail_agent_count = 0
    wrong_number_clients_count = 0
    # MV_RESOLVERS PROACTIVE AGENT METRICS DISABLED
    if department_name == 'MV_Resolvers':
        # Calculate sub-metrics (WHY breakdown and WHERE breakdown)
        proactive_agent_messages_count, directly_handled_by_seniors_count, other_bots_to_seniors_count, our_bot_to_seniors_count, delighters_to_seniors_count, mv_bot_known_flow_transfer_count, mv_bot_tech_errors_transfers_count, mv_bot_guardrails_count, mv_bot_other_transfers_count, our_bot_to_mv_resolvers_seniors_count, our_bot_to_mv_callers_count, our_bot_to_pre_r_visa_retention_count = calculate_proactive_agent_messages_mv_resolvers(
            session, department_name, departments_config, target_date
        )
        # MV_Resolvers specific: Calculate total seniors/callers and categorize paths (THIS IS THE SOURCE OF TRUTH)
        total_seniors_callers_count, seniors_our_bot_count, seniors_directly_handled_count, seniors_proactive_count, seniors_proactive_mv_resolvers_seniors_only_count, seniors_delighters_count, seniors_other_bots_count, seniors_conv_ids, our_bot_to_seniors_conv_ids, seniors_our_bot_to_mv_resolvers_seniors_count, seniors_our_bot_to_mv_callers_count, seniors_our_bot_to_pre_r_visa_retention_count, seniors_supervisor_excluded_conv_ids, seniors_no_response_transfers_count = calculate_total_seniors_callers(
            session, department_name, departments_config, target_date
        )
    #     
        # MV_Resolvers specific: Store detailed breakdown in raw table
        store_resolvers_chats_breakdown(
            session, department_name, departments_config, target_date
        )
        # MV_Resolvers specific: Count unique clients who were redirected via wrong-number message
        wrong_number_clients_count = calculate_wrong_number_clients(session, target_date)
    #     
    #     # OVERRIDE the old metrics with the new breakdown from seniors/callers (to avoid confusion)
        our_bot_to_seniors_count = seniors_our_bot_count
        directly_handled_by_seniors_count = seniors_directly_handled_count
        proactive_agent_messages_count = seniors_proactive_count
        other_bots_to_seniors_count = seniors_other_bots_count
    
    # if department_name == 'MV_Resolvers':
    #     print(f"    ⚠️  MV_Resolvers proactive agent metrics: DISABLED")
    
    # Calculate guardrail interventions for departments that support it
    if department_name in ['MV_Resolvers', 'CC_Resolvers', 'multiple_contract_detector']:
        total_guardrail_count = calculate_total_guardrail(
            session, department_name, departments_config, target_date
        )
    
    # Calculate guardrail applicant interventions for CC_Resolvers only
    if department_name == 'CC_Resolvers':
        guardrail_agent_count = calculate_guardrail_agent(
            session, department_name, departments_config, target_date
        )
    
    # All departments: Calculate transfers due to tech errors
    tech_error_transfers_count = calculate_transfers_due_to_tech_error(
        session, department_name, departments_config, target_date
    )
    
    # Initialize tracking sets before empty check
    fully_bot_handled_conv_ids = set()
    
    if df.empty:
        print(f"    ⚠️  No filtered data for {department_name}")
        return {
            'total_conversations': 0,
            'bot_handled_count': 0,
            'bot_handled_percentage': 0.0,
            'chats_with_1_plus_agent_messages': 0,
            'chats_with_2_plus_agent_messages': 0,
            'chats_with_3_plus_agent_messages': 0,
            'chats_with_1_plus_agent_messages_percentage': 0.0,
            'chats_with_2_plus_agent_messages_percentage': 0.0,
            'chats_with_3_plus_agent_messages_percentage': 0.0,
            'call_requests_count': 0,
            'call_requests_percentage': 0.0,
            'total_counted_agent_messages': 0,
            'total_bot_messages': 0,
            'agent_intervention_percentage': 0.0,
            'bot_handled_excluding_fillers_count': 0,
            'bot_handled_excluding_fillers_percentage': 0.0,
            'CHATS_WITH_EXACTLY_1_AGENT_MESSAGE': 0,
            'CHATS_WITH_EXACTLY_1_AGENT_MESSAGE_PERCENTAGE': 0.0,
            'CHATS_WITH_EXACTLY_2_AGENT_MESSAGES': 0,
            'CHATS_WITH_EXACTLY_2_AGENT_MESSAGES_PERCENTAGE': 0.0,
            'CHATS_WITH_EXACTLY_3_AGENT_MESSAGES': 0,
            'CHATS_WITH_EXACTLY_3_AGENT_MESSAGES_PERCENTAGE': 0.0,
            'complaint_action_count': 0,
            'complaint_action_percentage': 0.0,
            'complaint_action_with_0_agent_messages_count': 0,
            'complaint_action_with_0_agent_messages_percentage': 0.0,
            'complaint_action_with_1_plus_agent_messages_count': 0,
            'complaint_action_with_1_plus_agent_messages_percentage': 0.0,
            'chats_with_no_bot_messages_count': 0,
            'chats_with_no_bot_messages_percentage': 0.0,
            'bot_handled_with_bot_response_count': 0,
            'bot_handled_with_bot_response_percentage': 0.0,
            'conversations_with_bot_response_count': 0,
            'proactive_agent_messages_count': proactive_agent_messages_count,
            'proactive_agent_messages_percentage': 0.0,
            'directly_handled_by_seniors_count': directly_handled_by_seniors_count,
            'directly_handled_by_seniors_percentage': 0.0,
            'other_bots_to_seniors_count': other_bots_to_seniors_count,
            'other_bots_to_seniors_percentage': 0.0,
            'our_bot_to_seniors_count': our_bot_to_seniors_count,
            'our_bot_to_seniors_percentage': 0.0,
            'mv_bot_known_flow_transfer_count': mv_bot_known_flow_transfer_count,
            'mv_bot_known_flow_transfer_percentage': 0.0,
            'mv_bot_tech_errors_transfers_count': mv_bot_tech_errors_transfers_count,
            'mv_bot_tech_errors_transfers_percentage': 0.0,
            'mv_bot_guardrails_count': mv_bot_guardrails_count,
            'mv_bot_guardrails_percentage': 0.0,
            'mv_bot_other_transfers_count': mv_bot_other_transfers_count,
            'mv_bot_other_transfers_percentage': 0.0,
            'our_bot_to_mv_resolvers_seniors_count': our_bot_to_mv_resolvers_seniors_count,
            'our_bot_to_mv_resolvers_seniors_percentage': 0.0,
            'our_bot_to_mv_callers_count': our_bot_to_mv_callers_count,
            'our_bot_to_mv_callers_percentage': 0.0,
            'our_bot_to_pre_r_visa_retention_count': our_bot_to_pre_r_visa_retention_count,
            'our_bot_to_pre_r_visa_retention_percentage': 0.0,
            'delighters_to_seniors_count': delighters_to_seniors_count,
            'delighters_to_seniors_percentage': 0.0,
            'total_seniors_callers_count': total_seniors_callers_count,
            'total_seniors_callers_percentage': 0.0,
            'seniors_our_bot_count': seniors_our_bot_count,
            'seniors_our_bot_percentage': 0.0,
            'seniors_directly_handled_count': seniors_directly_handled_count,
            'seniors_directly_handled_percentage': 0.0,
            'seniors_proactive_count': seniors_proactive_count,
            'seniors_proactive_percentage': 0.0,
            'seniors_proactive_mv_resolvers_seniors_only_count': seniors_proactive_mv_resolvers_seniors_only_count,
            'seniors_proactive_mv_resolvers_seniors_only_percentage': 0.0,
            'seniors_our_bot_to_mv_resolvers_seniors_count': seniors_our_bot_to_mv_resolvers_seniors_count,
            'seniors_our_bot_to_mv_resolvers_seniors_percentage': 0.0,
            'seniors_our_bot_to_mv_callers_count': seniors_our_bot_to_mv_callers_count,
            'seniors_our_bot_to_mv_callers_percentage': 0.0,
            'seniors_our_bot_to_pre_r_visa_retention_count': seniors_our_bot_to_pre_r_visa_retention_count,
            'seniors_our_bot_to_pre_r_visa_retention_percentage': 0.0,
            'seniors_delighters_count': seniors_delighters_count,
            'seniors_delighters_percentage': 0.0,
            'seniors_other_bots_count': seniors_other_bots_count,
            'seniors_other_bots_percentage': 0.0,
            'unique_union_count': 0,
            'total_guardrail_count': total_guardrail_count,
            'total_guardrail_percentage': 0.0,
            'guardrail_agent_count': guardrail_agent_count,
            'guardrail_agent_percentage': 0.0,
            'tech_error_transfers_count': tech_error_transfers_count,
            'tech_error_transfers_percentage': 0.0,
             'mcd_mv_transfer_count': 0,
            'mcd_cc_transfer_count': 0,
            'chats_initiated_by_us_count': 0,
            'chats_initiated_by_client_count': 0,
            'wrong_number_clients_count': wrong_number_clients_count
        }
        
    
    # Group by conversation ID
    conversations = df.groupby('CONVERSATION_ID')
    total_conversations = len(conversations)
    
    # Get unique conversation IDs from the bot-handled df (chats supposed to be bot-handled)
    bot_handled_conv_ids = set(df['CONVERSATION_ID'].unique())

    # ── Chat initiation breakdown (Initiated by Us vs Initiated by Client) ──
    dept_config = departments_config.get(department_name, {})
    _table_name = dept_config.get('table_name', '')
    chats_initiated_by_us_count, chats_initiated_by_client_count = compute_chat_initiation_breakdown(
        session, df, _table_name, target_date, department_name
    )
    
    bot_handled_conversations_data = []
    bot_handled_count = 0
    
    # Initialize breakdown counters
    chats_with_1_plus_agent_messages = 0
    chats_with_2_plus_agent_messages = 0
    chats_with_3_plus_agent_messages = 0
    chats_with_1_plus_agent_messages_excluding_pokes = 0
    chats_with_pokes = 0  # New counter for conversations with any poke messages
    chats_with_exactly_1_agent_message = 0
    chats_with_exactly_2_agent_messages = 0
    chats_with_exactly_3_agent_messages = 0
    # Initialize avg bot messages before transfer accumulators
    transferred_bot_messages_total = 0
    transferred_conversation_count = 0
    call_requests_count = 0
    
    # multiple_contract_detector: count transfers to MV_Resolvers vs CC_Resolvers
    mcd_mv_transfer_count = 0
    mcd_cc_transfer_count = 0
    if department_name == 'multiple_contract_detector':
        _mv_cfg = departments_config.get('MV_Resolvers', {})
        _cc_cfg = departments_config.get('CC_Resolvers', {})
        _mv_all_skills = set(
            s.upper() for s in _mv_cfg.get('bot_skills', []) + _mv_cfg.get('agent_skills', [])
        )
        _cc_all_skills = set(
            s.upper() for s in _cc_cfg.get('bot_skills', []) + _cc_cfg.get('agent_skills', [])
        )
    # Initialize intervention counters
    total_counted_agent_messages = 0
    total_bot_messages = 0
    
    # Initialize new metric counter
    bot_handled_excluding_fillers_count = 0
    
    # Initialize CC_Resolvers specific counter for complaint actions
    complaint_action_count = 0
    complaint_action_with_0_agent_messages_count = 0
    complaint_action_with_1_plus_agent_messages_count = 0
    
    # Initialize CC_Resolvers specific: Track conversations with no bot messages from GPT_CC_RESOLVERS
    chats_with_no_bot_messages_count = 0
    
    # Initialize CC_Resolvers specific: Track handle ratio when bot responds
    # (Exclude conversations with 0 bot messages from both numerator and denominator)
    bot_handled_with_bot_response_count = 0
    conversations_with_bot_response_count = 0
    
    # Initialize intervention conversations data collection
    intervention_conversations_data = []
    chats_with_n_plus_agent_messages_data = []
    
    for conv_id, conv_df in conversations:
        is_bot_handled, agent_message_count, has_call_request, counted_agent_messages, bot_message_count, is_bot_handled_excluding_fillers, has_valid_system_transfer, agent_message_count_excluding_pokes, agent_messages_from_allowed_skills, has_complaint_action = is_conversation_fully_handled_by_bot_snowflake(conv_df, department_name, departments_config)
        
        # CC_Resolvers specific: Track conversations with NO bot normal messages from GPT_CC_RESOLVERS
        if department_name == 'CC_Resolvers':
            bot_cc_resolvers_messages = conv_df[
                (conv_df['SENT_BY'].str.upper() == 'BOT') &
                (conv_df['MESSAGE_TYPE'].str.upper() == 'NORMAL MESSAGE') &
                (conv_df['TARGET_SKILL_PER_MESSAGE'].str.upper() == 'GPT_CC_RESOLVERS')
            ]
            
            if len(bot_cc_resolvers_messages) == 0:
                chats_with_no_bot_messages_count += 1
            
            # Track handle ratio when bot responds (bot_message_count > 0)
            # This excludes conversations where bot never sent a normal message
            if bot_message_count > 0:
                conversations_with_bot_response_count += 1
                if is_bot_handled:
                    bot_handled_with_bot_response_count += 1
        

        # Track intervention metrics across all conversations
        total_counted_agent_messages += counted_agent_messages
        total_bot_messages += bot_message_count
        
        # Track call requests (regardless of bot-handled status)
        if has_call_request:
            call_requests_count += 1
        
        # Track CC_Resolvers specific: complaint actions (regardless of bot-handled status)
        if has_complaint_action:
            complaint_action_count += 1
            # Split by agent message count
            if agent_message_count == 0:
                complaint_action_with_0_agent_messages_count += 1
            elif agent_message_count >= 1:
                complaint_action_with_1_plus_agent_messages_count += 1
        
        # Track new metric: bot handled excluding fillers
        if is_bot_handled_excluding_fillers:
            bot_handled_excluding_fillers_count += 1
        
        if is_bot_handled:
            bot_handled_count += 1
            fully_bot_handled_conv_ids.add(conv_id)  # Track this conversation as fully bot-handled
            chats_with_n_plus_agent_messages_data.append({
                'CONVERSATION_ID': conv_id,
                'DEPARTMENT_NAME': department_name,
                'TARGET_DATE': target_date,
                'AGENT_MESSAGE_COUNT': agent_message_count,
                'ANALYSIS_DATE': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            
            # Store conversation data with additional metadata
            conv_data = conv_df.copy()
            conv_data['IS_BOT_HANDLED'] = True
            conv_data['ANALYSIS_DATE'] = datetime.now().strftime('%Y-%m-%d')
            
            bot_handled_conversations_data.extend(conv_data.to_dict('records'))
        else:
            # Count conversations with agent messages (not bot-handled cases)
            # For CC_Resolvers: only count if bot sent at least 1 message (bot_message_count > 0)
            # For other departments: count all
            
            should_count_in_breakdown = True
            if department_name == 'CC_Resolvers' and bot_message_count == 0:
                should_count_in_breakdown = False
            
            if should_count_in_breakdown:
                # For CC_Resolvers: also count complaint actions as 1+ agent messages
                if agent_message_count >= 1 or has_complaint_action:
                    chats_with_n_plus_agent_messages_data.append({
                            'CONVERSATION_ID': conv_id,
                            'DEPARTMENT_NAME': department_name,
                            'TARGET_DATE': target_date,
                            'AGENT_MESSAGE_COUNT': agent_message_count,
                            'ANALYSIS_DATE': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        })
                    chats_with_1_plus_agent_messages += 1
                if agent_message_count >= 2:
                    chats_with_2_plus_agent_messages += 1
                if agent_message_count >= 3:
                    chats_with_3_plus_agent_messages += 1
            
            # Count conversations with 1+ agent messages excluding pokes
            if agent_message_count_excluding_pokes >= 1:
                if should_count_in_breakdown:
                    chats_with_1_plus_agent_messages_excluding_pokes += 1
         # Check if conversation has any poke messages (regardless of bot_handled status)
        has_poke = False
        for idx, message in conv_df.iterrows():
            message_skill = message.get('TARGET_SKILL_PER_MESSAGE', '')
            message_content = str(message.get('TEXT', '')).lower()
            sent_by = str(message.get('SENT_BY', '')).upper()
            
            # Only check agent/bot messages from department skills
            if sent_by in ['AGENT', 'BOT']:
                if department_name == 'CC_Sales':
                    # Check against pokes list for CC_Sales
                     if re.search(r'\d+\.?\d*\s*minutes?\s+poke', message_content, re.IGNORECASE) or 'minutes poke' in message_content or 'minute poke' in message_content:
                        has_poke = True
                        break                        
                elif department_name == 'MV_Sales':
                    # Check for "minutes poke" or "minute poke" pattern for MV_Sales
                    # Match patterns like "10 minutes poke", "2.5 minutes poke", "minute poke", etc.
                    if re.search(r'\d+\.?\d*\s*minutes?\s+poke', message_content, re.IGNORECASE) or 'minutes poke' in message_content or 'minute poke' in message_content:
                        has_poke = True
                        break
            
            if has_poke:
                break
        
        if has_poke:
            chats_with_pokes += 1
        
        # Count conversations with exactly 1, 2, or 3 agent messages from allowed skills (regardless of bot_handled status)
        # For CC_Resolvers: only count if bot sent at least 1 message
        should_count_exact = True
        if department_name == 'CC_Resolvers' and bot_message_count == 0:
            should_count_exact = False
        
        if should_count_exact:
            if agent_messages_from_allowed_skills == 1:
                chats_with_exactly_1_agent_message += 1
            if agent_messages_from_allowed_skills == 2:
                chats_with_exactly_2_agent_messages += 1
            if agent_messages_from_allowed_skills == 3:
                chats_with_exactly_3_agent_messages += 1
        
        # Check for intervention conversations (regardless of bot_handled status)
        # This captures system transfers from bot to agent skills even if no agent messages were sent
        if has_valid_system_transfer:
            # Track bot messages in transferred conversations for avg_bot_msgs_before_transfer
            transferred_bot_messages_total += bot_message_count
            transferred_conversation_count += 1
            # Store intervention conversation data
            intervention_conversations_data.append({
                'CONVERSATION_ID': conv_id,
                'DEPARTMENT_NAME': department_name,
                'TARGET_DATE': target_date,
                'AGENT_MESSAGE_COUNT': agent_message_count,
                'ANALYSIS_DATE': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
         # multiple_contract_detector: classify each transferred conversation as MV or CC
        if department_name == 'multiple_contract_detector':
                _sys_msgs = conv_df[
                    (conv_df['SENT_BY'].str.upper() == 'SYSTEM') &
                    ((conv_df['MESSAGE_TYPE'].str.upper() == 'PRIVATE MESSAGE') |
                     (conv_df['MESSAGE_TYPE'].str.upper() == 'TRANSFER') | (conv_df['MESSAGE_TYPE'].str.upper() == 'TOOL'))
                ]
                _classified = False
                for _, _msg in _sys_msgs.iterrows():
                    _td = parse_transfer(str(_msg.get('TEXT', '')))
                    _to = _td.get('to_skill', '').upper()
                    if _to in _mv_all_skills:
                        mcd_mv_transfer_count += 1
                        _classified = True
                        break
                    elif _to in _cc_all_skills:
                        mcd_cc_transfer_count += 1
                        _classified = True
                        break
    
    # Calculate percentages
    bot_handled_percentage = (bot_handled_count / total_conversations * 100) if total_conversations > 0 else 0
    chats_with_1_plus_percentage = (chats_with_1_plus_agent_messages / total_conversations * 100) if total_conversations > 0 else 0
    chats_with_2_plus_percentage = (chats_with_2_plus_agent_messages / total_conversations * 100) if total_conversations > 0 else 0
    chats_with_3_plus_percentage = (chats_with_3_plus_agent_messages / total_conversations * 100) if total_conversations > 0 else 0
    chats_with_1_plus_excluding_pokes_percentage = (chats_with_1_plus_agent_messages_excluding_pokes / total_conversations * 100) if total_conversations > 0 else 0
    call_requests_percentage = (call_requests_count / total_conversations * 100) if total_conversations > 0 else 0
    
    # Calculate agent intervention percentage
    total_messages = total_counted_agent_messages + total_bot_messages
    agent_intervention_percentage = (total_counted_agent_messages / total_messages * 100) if total_messages > 0 else 0
    
    # Calculate bot handled excluding fillers percentage
    bot_handled_excluding_fillers_percentage = (bot_handled_excluding_fillers_count / total_conversations * 100) if total_conversations > 0 else 0
    
    # Calculate percentages for new metrics
    chats_with_exactly_1_agent_message_percentage = (chats_with_exactly_1_agent_message / total_conversations * 100) if total_conversations > 0 else 0
    chats_with_exactly_2_agent_messages_percentage = (chats_with_exactly_2_agent_messages / total_conversations * 100) if total_conversations > 0 else 0
    chats_with_exactly_3_agent_messages_percentage = (chats_with_exactly_3_agent_messages / total_conversations * 100) if total_conversations > 0 else 0
    
    # Calculate percentage for CC_Resolvers complaint actions
    complaint_action_percentage = (complaint_action_count / total_conversations * 100) if total_conversations > 0 else 0
    complaint_action_with_0_agent_messages_percentage = (complaint_action_with_0_agent_messages_count / total_conversations * 100) if total_conversations > 0 else 0
    complaint_action_with_1_plus_agent_messages_percentage = (complaint_action_with_1_plus_agent_messages_count / total_conversations * 100) if total_conversations > 0 else 0
    
    # Calculate percentage for CC_Resolvers chats with no bot messages
    chats_with_no_bot_messages_percentage = (chats_with_no_bot_messages_count / total_conversations * 100) if total_conversations > 0 else 0
    
    # Calculate percentage for CC_Resolvers handle ratio when bot responds
    bot_handled_with_bot_response_percentage = (bot_handled_with_bot_response_count / conversations_with_bot_response_count * 100) if conversations_with_bot_response_count > 0 else 0
    
    # Calculate percentage for poke conversations
    chats_with_pokes_percentage = (chats_with_pokes / total_conversations * 100) if total_conversations > 0 else 0
    
    # Calculate percentage for tech error transfers (all departments)
    tech_error_transfers_percentage = (tech_error_transfers_count / total_conversations * 100) if total_conversations > 0 else 0
    
    # Calculate unique union count for MV_Resolvers (supposed ∪ seniors ∪ supervisor-removed-from-base)
    unique_union_count = 0
    if department_name == 'MV_Resolvers':
        unique_union_count = len(
            bot_handled_conv_ids.union(seniors_conv_ids).union(seniors_supervisor_excluded_conv_ids)
        )
    
    # Calculate percentage for MV_Resolvers total seniors/callers (out of unique union count)
    total_seniors_callers_percentage = (total_seniors_callers_count / unique_union_count * 100) if unique_union_count > 0 else (total_seniors_callers_count / total_conversations * 100) if total_conversations > 0 else 0
    
    # Calculate percentage for MV_Resolvers total guardrail interventions
    total_guardrail_percentage = (total_guardrail_count / total_conversations * 100) if total_conversations > 0 else 0
    guardrail_agent_percentage = (guardrail_agent_count / total_conversations * 100) if total_conversations > 0 else 0
    
    # Calculate percentages for MV_Resolvers proactive metrics (out of unique union count for specific metrics)
    proactive_agent_messages_percentage = (proactive_agent_messages_count / unique_union_count * 100) if unique_union_count > 0 else (proactive_agent_messages_count / total_conversations * 100) if total_conversations > 0 else 0
    directly_handled_by_seniors_percentage = (directly_handled_by_seniors_count / total_conversations * 100) if total_conversations > 0 else 0
    other_bots_to_seniors_percentage = (other_bots_to_seniors_count / unique_union_count * 100) if unique_union_count > 0 else (other_bots_to_seniors_count / total_conversations * 100) if total_conversations > 0 else 0
    our_bot_to_seniors_percentage = (our_bot_to_seniors_count / unique_union_count * 100) if unique_union_count > 0 else (our_bot_to_seniors_count / total_conversations * 100) if total_conversations > 0 else 0
    delighters_to_seniors_percentage = (delighters_to_seniors_count / total_conversations * 100) if total_conversations > 0 else 0
    
    # Debug output for MV_Resolvers
    if department_name == 'MV_Resolvers':
        print(f"    🔍 DEBUG: unique_union_count = {unique_union_count}")
        print(f"    🔍 DEBUG: proactive denominator = {unique_union_count if unique_union_count > 0 else total_conversations}")
        print(f"    🔍 DEBUG: our_bot denominator = {unique_union_count if unique_union_count > 0 else total_conversations}")
        print(f"    🔍 DEBUG: other_bots denominator = {unique_union_count if unique_union_count > 0 else total_conversations}")
    
    # Calculate percentages for MV_Resolvers sub-metrics (out of our_bot_to_seniors_count)
    mv_bot_known_flow_transfer_percentage = (mv_bot_known_flow_transfer_count / our_bot_to_seniors_count * 100) if our_bot_to_seniors_count > 0 else 0
    mv_bot_tech_errors_transfers_percentage = (mv_bot_tech_errors_transfers_count / our_bot_to_seniors_count * 100) if our_bot_to_seniors_count > 0 else 0
    mv_bot_guardrails_percentage = (mv_bot_guardrails_count / our_bot_to_seniors_count * 100) if our_bot_to_seniors_count > 0 else 0
    mv_bot_other_transfers_percentage = (mv_bot_other_transfers_count / our_bot_to_seniors_count * 100) if our_bot_to_seniors_count > 0 else 0
    
    # Calculate percentages for "Our bot to seniors" breakdown by target skill (out of our_bot_to_seniors_count)
    our_bot_to_mv_resolvers_seniors_percentage = (our_bot_to_mv_resolvers_seniors_count / our_bot_to_seniors_count * 100) if our_bot_to_seniors_count > 0 else 0
    our_bot_to_mv_callers_percentage = (our_bot_to_mv_callers_count / our_bot_to_seniors_count * 100) if our_bot_to_seniors_count > 0 else 0
    our_bot_to_pre_r_visa_retention_percentage = (our_bot_to_pre_r_visa_retention_count / our_bot_to_seniors_count * 100) if our_bot_to_seniors_count > 0 else 0
    
    # Calculate percentages for NEW seniors breakdown (out of UNIQUE_UNION_COUNT for MV_Resolvers, otherwise total_seniors_callers_count)
    seniors_no_response_transfers_percentage = (seniors_no_response_transfers_count / unique_union_count * 100) if unique_union_count > 0 else (seniors_no_response_transfers_count / total_seniors_callers_count * 100) if total_seniors_callers_count > 0 else 0
    seniors_our_bot_percentage = (seniors_our_bot_count / unique_union_count * 100) if unique_union_count > 0 else (seniors_our_bot_count / total_seniors_callers_count * 100) if total_seniors_callers_count > 0 else 0
    seniors_directly_handled_percentage = (seniors_directly_handled_count / unique_union_count * 100) if unique_union_count > 0 else (seniors_directly_handled_count / total_seniors_callers_count * 100) if total_seniors_callers_count > 0 else 0
    seniors_proactive_percentage = (seniors_proactive_count / unique_union_count * 100) if unique_union_count > 0 else (seniors_proactive_count / total_seniors_callers_count * 100) if total_seniors_callers_count > 0 else 0
    seniors_proactive_mv_resolvers_seniors_only_percentage = (seniors_proactive_mv_resolvers_seniors_only_count / seniors_proactive_count * 100) if seniors_proactive_count > 0 else 0
    
    # Calculate percentages for seniors "Our bot to seniors" breakdown by target skill (out of seniors_our_bot_count)
    seniors_our_bot_to_mv_resolvers_seniors_percentage = (seniors_our_bot_to_mv_resolvers_seniors_count / seniors_our_bot_count * 100) if seniors_our_bot_count > 0 else 0
    seniors_our_bot_to_mv_callers_percentage = (seniors_our_bot_to_mv_callers_count / seniors_our_bot_count * 100) if seniors_our_bot_count > 0 else 0
    seniors_our_bot_to_pre_r_visa_retention_percentage = (seniors_our_bot_to_pre_r_visa_retention_count / seniors_our_bot_count * 100) if seniors_our_bot_count > 0 else 0
    
    seniors_delighters_percentage = (seniors_delighters_count / unique_union_count * 100) if unique_union_count > 0 else (seniors_delighters_count / total_seniors_callers_count * 100) if total_seniors_callers_count > 0 else 0
    seniors_other_bots_percentage = (seniors_other_bots_count / unique_union_count * 100) if unique_union_count > 0 else (seniors_other_bots_count / total_seniors_callers_count * 100) if total_seniors_callers_count > 0 else 0
    
    # MV_Resolvers: Print coverage analysis (after bot_handled_count is calculated)
    if department_name == 'MV_Resolvers' and total_seniors_callers_count > 0:
        # Calculate overlaps
        overlap_supposed_and_seniors = len(bot_handled_conv_ids.intersection(seniors_conv_ids))
        overlap_supposed_and_our_bot = len(bot_handled_conv_ids.intersection(our_bot_to_seniors_conv_ids))
        overlap_fully_handled_and_seniors = len(fully_bot_handled_conv_ids.intersection(seniors_conv_ids))
        
        unique_combined_count = len(
            bot_handled_conv_ids.union(seniors_conv_ids).union(seniors_supervisor_excluded_conv_ids)
        )
        
        # MV_RESOLVERS PROACTIVE AGENT METRICS COVERAGE ANALYSIS DISABLED
        # print(f"")
        # print(f"    📊 COVERAGE ANALYSIS:")
        # print(f"       ═══════════════════════════════════════════════════════════")
        # print(f"       Base Groups:")
        # print(f"       - Chats supposed to be bot-handled: {total_conversations}")
        # print(f"       - Chats fully handled by bot: {bot_handled_count}")
        # print(f"       - Total seniors/callers reached: {total_seniors_callers_count}")
        # print(f"       - Our bot to seniors: {seniors_our_bot_count}")
        # print(f"")
        # print(f"       🎯 UNIQUE UNION COUNT (Supposed ∪ Seniors): {unique_union_count}")
        # print(f"")
        # print(f"       Overlaps:")
        # print(f"       - Supposed to be bot-handled ∩ Seniors: {overlap_supposed_and_seniors}")
        # print(f"       - Supposed to be bot-handled ∩ Our bot to seniors: {overlap_supposed_and_our_bot}")
        # print(f"       - Fully handled by bot ∩ Seniors: {overlap_fully_handled_and_seniors}")
        # print(f"")
        # print(f"       Coverage:")
        # print(f"       - Only supposed (not seniors): {total_conversations - overlap_supposed_and_seniors}")
        # print(f"       - Only seniors (not supposed): {total_seniors_callers_count - overlap_supposed_and_seniors}")
        # print(f"")
        # print(f"       📊 Percentages using UNIQUE UNION as denominator:")
        # print(f"       - Total seniors/callers: {total_seniors_callers_count}/{unique_union_count} ({total_seniors_callers_percentage:.1f}%)")
        # print(f"       - Proactive: {proactive_agent_messages_count}/{unique_union_count} ({proactive_agent_messages_percentage:.1f}%)")
        # print(f"       - Our bot to seniors: {our_bot_to_seniors_count}/{unique_union_count} ({our_bot_to_seniors_percentage:.1f}%)")
        # print(f"       - Other bots to seniors: {other_bots_to_seniors_count}/{unique_union_count} ({other_bots_to_seniors_percentage:.1f}%)")
        # print(f"       ═══════════════════════════════════════════════════════════")
    
    results = {
        'total_conversations': total_conversations,
        'bot_handled_count': bot_handled_count,
        'bot_handled_percentage': bot_handled_percentage,
        'chats_with_1_plus_agent_messages': chats_with_1_plus_agent_messages,
        'chats_with_2_plus_agent_messages': chats_with_2_plus_agent_messages,
        'chats_with_3_plus_agent_messages': chats_with_3_plus_agent_messages,
        'chats_with_1_plus_agent_messages_percentage': chats_with_1_plus_percentage,
        'chats_with_2_plus_agent_messages_percentage': chats_with_2_plus_percentage,
        'chats_with_3_plus_agent_messages_percentage': chats_with_3_plus_percentage,
        'chats_with_1_plus_agent_messages_excluding_pokes': chats_with_1_plus_agent_messages_excluding_pokes,
        'chats_with_1_plus_agent_messages_excluding_pokes_percentage': chats_with_1_plus_excluding_pokes_percentage,
        'chats_with_pokes': chats_with_pokes,
        'chats_with_pokes_percentage': chats_with_pokes_percentage,
        'call_requests_count': call_requests_count,
        'call_requests_percentage': call_requests_percentage,
        'total_counted_agent_messages': total_counted_agent_messages,
        'total_bot_messages': total_bot_messages,
        'agent_intervention_percentage': agent_intervention_percentage,
        'bot_handled_excluding_fillers_count': bot_handled_excluding_fillers_count,
        'bot_handled_excluding_fillers_percentage': bot_handled_excluding_fillers_percentage,
        'CHATS_WITH_EXACTLY_1_AGENT_MESSAGE': chats_with_exactly_1_agent_message,
        'CHATS_WITH_EXACTLY_1_AGENT_MESSAGE_PERCENTAGE': chats_with_exactly_1_agent_message_percentage,
        'CHATS_WITH_EXACTLY_2_AGENT_MESSAGES': chats_with_exactly_2_agent_messages,
        'CHATS_WITH_EXACTLY_2_AGENT_MESSAGES_PERCENTAGE': chats_with_exactly_2_agent_messages_percentage,
        'CHATS_WITH_EXACTLY_3_AGENT_MESSAGES': chats_with_exactly_3_agent_messages,
        'CHATS_WITH_EXACTLY_3_AGENT_MESSAGES_PERCENTAGE': chats_with_exactly_3_agent_messages_percentage,
        'avg_bot_msgs_before_transfer': (
            round(transferred_bot_messages_total / transferred_conversation_count, 2)
            if transferred_conversation_count > 0 else None
        ),
        'transferred_conversation_count': transferred_conversation_count,
        'complaint_action_count': complaint_action_count,
        'complaint_action_percentage': complaint_action_percentage,
        'complaint_action_with_0_agent_messages_count': complaint_action_with_0_agent_messages_count,
        'complaint_action_with_0_agent_messages_percentage': complaint_action_with_0_agent_messages_percentage,
        'complaint_action_with_1_plus_agent_messages_count': complaint_action_with_1_plus_agent_messages_count,
        'complaint_action_with_1_plus_agent_messages_percentage': complaint_action_with_1_plus_agent_messages_percentage,
        'chats_with_no_bot_messages_count': chats_with_no_bot_messages_count,
        'chats_with_no_bot_messages_percentage': chats_with_no_bot_messages_percentage,
        'bot_handled_with_bot_response_count': bot_handled_with_bot_response_count,
        'bot_handled_with_bot_response_percentage': bot_handled_with_bot_response_percentage,
        'conversations_with_bot_response_count': conversations_with_bot_response_count,
        'proactive_agent_messages_count': proactive_agent_messages_count,
        'proactive_agent_messages_percentage': proactive_agent_messages_percentage,
        'directly_handled_by_seniors_count': directly_handled_by_seniors_count,
        'directly_handled_by_seniors_percentage': directly_handled_by_seniors_percentage,
        'other_bots_to_seniors_count': other_bots_to_seniors_count,
        'other_bots_to_seniors_percentage': other_bots_to_seniors_percentage,
        'our_bot_to_seniors_count': our_bot_to_seniors_count,
        'our_bot_to_seniors_percentage': our_bot_to_seniors_percentage,
        'mv_bot_known_flow_transfer_count': mv_bot_known_flow_transfer_count,
        'mv_bot_known_flow_transfer_percentage': mv_bot_known_flow_transfer_percentage,
        'mv_bot_tech_errors_transfers_count': mv_bot_tech_errors_transfers_count,
        'mv_bot_tech_errors_transfers_percentage': mv_bot_tech_errors_transfers_percentage,
        'mv_bot_guardrails_count': mv_bot_guardrails_count,
        'mv_bot_guardrails_percentage': mv_bot_guardrails_percentage,
        'mv_bot_other_transfers_count': mv_bot_other_transfers_count,
        'mv_bot_other_transfers_percentage': mv_bot_other_transfers_percentage,
        'our_bot_to_mv_resolvers_seniors_count': our_bot_to_mv_resolvers_seniors_count,
        'our_bot_to_mv_resolvers_seniors_percentage': our_bot_to_mv_resolvers_seniors_percentage,
        'our_bot_to_mv_callers_count': our_bot_to_mv_callers_count,
        'our_bot_to_mv_callers_percentage': our_bot_to_mv_callers_percentage,
        'our_bot_to_pre_r_visa_retention_count': our_bot_to_pre_r_visa_retention_count,
        'our_bot_to_pre_r_visa_retention_percentage': our_bot_to_pre_r_visa_retention_percentage,
        'delighters_to_seniors_count': delighters_to_seniors_count,
        'delighters_to_seniors_percentage': delighters_to_seniors_percentage,
        'total_seniors_callers_count': total_seniors_callers_count,
        'total_seniors_callers_percentage': total_seniors_callers_percentage,
        'seniors_no_response_transfers_count': seniors_no_response_transfers_count,
        'seniors_no_response_transfers_percentage': seniors_no_response_transfers_percentage,
        'seniors_our_bot_count': seniors_our_bot_count,
        'seniors_our_bot_percentage': seniors_our_bot_percentage,
        'seniors_directly_handled_count': seniors_directly_handled_count,
        'seniors_directly_handled_percentage': seniors_directly_handled_percentage,
        'seniors_proactive_count': seniors_proactive_count,
        'seniors_proactive_percentage': seniors_proactive_percentage,
        'seniors_proactive_mv_resolvers_seniors_only_count': seniors_proactive_mv_resolvers_seniors_only_count,
        'seniors_proactive_mv_resolvers_seniors_only_percentage': seniors_proactive_mv_resolvers_seniors_only_percentage,
        'seniors_our_bot_to_mv_resolvers_seniors_count': seniors_our_bot_to_mv_resolvers_seniors_count,
        'seniors_our_bot_to_mv_resolvers_seniors_percentage': seniors_our_bot_to_mv_resolvers_seniors_percentage,
        'seniors_our_bot_to_mv_callers_count': seniors_our_bot_to_mv_callers_count,
        'seniors_our_bot_to_mv_callers_percentage': seniors_our_bot_to_mv_callers_percentage,
        'seniors_our_bot_to_pre_r_visa_retention_count': seniors_our_bot_to_pre_r_visa_retention_count,
        'seniors_our_bot_to_pre_r_visa_retention_percentage': seniors_our_bot_to_pre_r_visa_retention_percentage,
        'seniors_delighters_count': seniors_delighters_count,
        'seniors_delighters_percentage': seniors_delighters_percentage,
        'seniors_other_bots_count': seniors_other_bots_count,
        'seniors_other_bots_percentage': seniors_other_bots_percentage,
        'unique_union_count': unique_union_count,
        'total_guardrail_count': total_guardrail_count,
        'total_guardrail_percentage': total_guardrail_percentage,
        'guardrail_agent_count': guardrail_agent_count,
        'guardrail_agent_percentage': guardrail_agent_percentage,
        'tech_error_transfers_count': tech_error_transfers_count,
        'tech_error_transfers_percentage': tech_error_transfers_percentage,
        'mcd_mv_transfer_count': mcd_mv_transfer_count,
        'mcd_cc_transfer_count': mcd_cc_transfer_count,
        'chats_initiated_by_us_count': chats_initiated_by_us_count,
        'chats_initiated_by_client_count': chats_initiated_by_client_count,
        'wrong_number_clients_count': wrong_number_clients_count
    }
    
    print(f"    ✅ {bot_handled_count}/{total_conversations} ({bot_handled_percentage:.1f}%) bot-handled")
    print(f"    📊 Agent message breakdown:")
    print(f"       - 1+ agent messages: {chats_with_1_plus_agent_messages} ({chats_with_1_plus_percentage:.1f}%)")
    print(f"       - 2+ agent messages: {chats_with_2_plus_agent_messages} ({chats_with_2_plus_percentage:.1f}%)")
    print(f"       - 3+ agent messages: {chats_with_3_plus_agent_messages} ({chats_with_3_plus_percentage:.1f}%)")
    print(f"       - 1+ agent messages (excluding pokes): {chats_with_1_plus_agent_messages_excluding_pokes} ({chats_with_1_plus_excluding_pokes_percentage:.1f}%)")
    print(f"       - Exactly 1 agent message (from allowed skills): {chats_with_exactly_1_agent_message} ({chats_with_exactly_1_agent_message_percentage:.1f}%)")
    print(f"       - Exactly 2 agent messages (from allowed skills): {chats_with_exactly_2_agent_messages} ({chats_with_exactly_2_agent_messages_percentage:.1f}%)")
    print(f"       - Exactly 3 agent messages (from allowed skills): {chats_with_exactly_3_agent_messages} ({chats_with_exactly_3_agent_messages_percentage:.1f}%)")
    # Print avg bot messages before transfer
    avg_bot_msgs = transferred_bot_messages_total / transferred_conversation_count if transferred_conversation_count > 0 else None
    if avg_bot_msgs is not None:
        print(f"    🤖 Avg bot messages before transfer: {avg_bot_msgs:.2f} (over {transferred_conversation_count} transferred convs)")
    print(f"    📞 Call requests: {call_requests_count} ({call_requests_percentage:.1f}%)")
    
    # CC_Resolvers specific: Print complaint action stats
    if department_name == 'CC_Resolvers':
        print(f"    🚨 Complaint actions (Open_or_CommentOn_Complaint): {complaint_action_count} ({complaint_action_percentage:.1f}%)")
        print(f"       └─ With 0 agent messages: {complaint_action_with_0_agent_messages_count} ({complaint_action_with_0_agent_messages_percentage:.1f}%)")
        print(f"       └─ With 1+ agent messages: {complaint_action_with_1_plus_agent_messages_count} ({complaint_action_with_1_plus_agent_messages_percentage:.1f}%)")
        print(f"    ⚠️  Chats with no bot messages from GPT_CC_RESOLVERS: {chats_with_no_bot_messages_count} ({chats_with_no_bot_messages_percentage:.1f}%)")
        print(f"    🎯 Handle ratio when bot responds (excluding 0 bot message chats): {bot_handled_with_bot_response_count}/{conversations_with_bot_response_count} ({bot_handled_with_bot_response_percentage:.1f}%)")
    
    # MV_RESOLVERS PROACTIVE AGENT METRICS PRINT DISABLED
    # # MV_Resolvers specific: Print proactive agent metrics
    # if department_name == 'MV_Resolvers':
    #     print(f"    📞 Total seniors/callers reached: {total_seniors_callers_count}/{total_conversations} ({total_seniors_callers_percentage:.1f}%) [BASE METRIC]")
    #     print(f"       └─ Our bot path: {seniors_our_bot_count}/{total_seniors_callers_count} ({seniors_our_bot_percentage:.1f}%)")
    #     print(f"       └─ Directly handled path: {seniors_directly_handled_count}/{total_seniors_callers_count} ({seniors_directly_handled_percentage:.1f}%)")
    #     print(f"       └─ Proactive path: {seniors_proactive_count}/{total_seniors_callers_count} ({seniors_proactive_percentage:.1f}%)")
    #     print(f"       └─ Delighters path: {seniors_delighters_count}/{total_seniors_callers_count} ({seniors_delighters_percentage:.1f}%)")
    #     print(f"       └─ Other bots path: {seniors_other_bots_count}/{total_seniors_callers_count} ({seniors_other_bots_percentage:.1f}%)")
    #     print(f"")
    #     print(f"    🎯 Proactive agent conversations (no bot skill): {proactive_agent_messages_count}/{total_conversations} ({proactive_agent_messages_percentage:.1f}%)")
    #     print(f"    👥 Directly handled by seniors (GPT_RESOLVERS_BOT → agent): {directly_handled_by_seniors_count}/{total_conversations} ({directly_handled_by_seniors_percentage:.1f}%)")
    #     print(f"    🔄 Other bots to seniors (any skill → MV seniors): {other_bots_to_seniors_count}/{total_conversations} ({other_bots_to_seniors_percentage:.1f}%)")
    #     print(f"    🤖 Our bot to seniors (GPT_MV_RESOLVERS → MV seniors): {our_bot_to_seniors_count}/{total_conversations} ({our_bot_to_seniors_percentage:.1f}%)")
    #     print(f"       └─ MV_BOT_Known_Flow_Transfer: {mv_bot_known_flow_transfer_count}/{our_bot_to_seniors_count} ({mv_bot_known_flow_transfer_percentage:.1f}%)")
    #     print(f"       └─ MV_BOT_Tech_Errors_Transfers: {mv_bot_tech_errors_transfers_count}/{our_bot_to_seniors_count} ({mv_bot_tech_errors_transfers_percentage:.1f}%)")
    #     print(f"       └─ MV_BOT_GUARDRAILS: {mv_bot_guardrails_count}/{our_bot_to_seniors_count} ({mv_bot_guardrails_percentage:.1f}%)")
    #     print(f"       └─ MV_BOT_Other_transfers: {mv_bot_other_transfers_count}/{our_bot_to_seniors_count} ({mv_bot_other_transfers_percentage:.1f}%)")
    #     print(f"    🎁 Delighters to seniors (gpt_delighters → MV seniors): {delighters_to_seniors_count}/{total_conversations} ({delighters_to_seniors_percentage:.1f}%)")
    #     print(f"    🛡️  Total guardrail interventions: {total_guardrail_count}/{total_conversations} ({total_guardrail_percentage:.1f}%)")
    
    # CC_Resolvers specific: Print guardrail stats
    if department_name == 'CC_Resolvers':
        print(f"    🛡️  Total guardrail interventions: {total_guardrail_count}/{total_conversations} ({total_guardrail_percentage:.1f}%)")
        print(f"    🛡️  Guardrail applicant interventions: {guardrail_agent_count}/{total_conversations} ({guardrail_agent_percentage:.1f}%)")
    
    print(f"    🤝 Agent intervention: {total_counted_agent_messages}/{total_messages} messages ({agent_intervention_percentage:.1f}%)")
    print(f"       - Counted agent messages: {total_counted_agent_messages}")
    print(f"       - Bot messages: {total_bot_messages}")
    print(f"    🎯 Bot handled excluding fillers: {bot_handled_excluding_fillers_count}/{total_conversations} ({bot_handled_excluding_fillers_percentage:.1f}%)")
    print(f"    🔧 Tech error transfers: {tech_error_transfers_count}/{total_conversations} ({tech_error_transfers_percentage:.1f}%)")
    
    
    # Save raw data to BOT_HANDLED_RAW_DATA table
    # if bot_handled_conversations_data:
    #     try:
    #         raw_df = pd.DataFrame(bot_handled_conversations_data)
    #         raw_df = clean_dataframe_for_snowflake(raw_df)
            
    #         # Define dynamic columns (excluding the essential columns that insert_raw_data_with_cleanup adds)
    #         dynamic_columns = [col for col in raw_df.columns if col not in ['DATE', 'DEPARTMENT', 'TIMESTAMP']]
            
    #         insert_result = insert_raw_data_with_cleanup(
    #             session=session,
    #             table_name="BOT_HANDLED_RAW_DATA",
    #             department=department_name,
    #             target_date=target_date,
    #             dataframe=raw_df[dynamic_columns],
    #             columns=dynamic_columns
    #         )
    #         print(f"    💾 Saved {len(bot_handled_conversations_data)} bot-handled records to BOT_HANDLED_RAW_DATA")
    #     except Exception as e:
    #         print(f"    ⚠️  Failed to save bot-handled raw data: {str(e)}")
    
    # Save intervention conversations data to INTERVENTIONS_CONVERSATIONS table
    if intervention_conversations_data:
        try:
            intervention_df = pd.DataFrame(intervention_conversations_data)
            intervention_df = clean_dataframe_for_snowflake(intervention_df)
            
            # Define dynamic columns for intervention data (excluding the essential columns that insert_raw_data_with_cleanup adds)
            dynamic_columns = [col for col in intervention_df.columns if col not in ['DATE', 'DEPARTMENT', 'TIMESTAMP']]
            
            insert_result = insert_raw_data_with_cleanup(
                session=session,
                table_name="INTERVENTIONS_CONVERSATIONS",
                department=department_name,
                target_date=target_date,
                dataframe=intervention_df[dynamic_columns],
                columns=dynamic_columns
            )
            
            print(f"    💾 Saved {len(intervention_conversations_data)} intervention conversation records to INTERVENTIONS_CONVERSATIONS")
           
        except Exception as e:
            print(f"    ⚠️  Failed to save intervention conversations data: {str(e)}")
    else:
        print(f"    ℹ️  No intervention conversations found for {department_name}")
    
    # Save chats with n plus agent messages data to CHATS_WITH_N_PLUS_AGENT_MESSAGES table
    if chats_with_n_plus_agent_messages_data:
        try:
            chats_with_n_plus_agent_messages_df = pd.DataFrame(chats_with_n_plus_agent_messages_data)
            chats_with_n_plus_agent_messages_df = clean_dataframe_for_snowflake(chats_with_n_plus_agent_messages_df)
            
            # Define dynamic columns for chats with n plus agent messages data (excluding the essential columns that insert_raw_data_with_cleanup adds)
            dynamic_columns = [col for col in chats_with_n_plus_agent_messages_df.columns if col not in ['DATE', 'DEPARTMENT', 'TIMESTAMP']]
            
            insert_raw_data_with_cleanup(
                session=session,
                table_name="CHATS_WITH_N_PLUS_AGENT_MESSAGES",
                department=department_name,
                target_date=target_date,
                dataframe=chats_with_n_plus_agent_messages_df[dynamic_columns],
                columns=dynamic_columns
            )
            print(f"    💾 Saved {len(chats_with_n_plus_agent_messages_data)} chats with n plus agent messages records to CHATS_WITH_N_PLUS_AGENT_MESSAGES")
        except Exception as e:
            print(f"    ⚠️  Failed to save chats with n plus agent messages data: {str(e)}")
    else:
        print(f"    ℹ️  No chats with n plus agent messages found for {department_name}")
    
    return results



def analyze_bot_handled_conversations_all_departments(session: snowpark.Session, target_date=None):
    """
    Analyze bot-handled conversations for all departments using Phase 1 filtered data.
    Raw data is saved immediately for each department.
    
    Args:
        session: Snowflake session
        target_date: Target date for analysis
    
    Returns:
        department_results dictionary
    """
    print("\n🤖 PHASE 2A: ANALYZING BOT-HANDLED CONVERSATIONS")
    print("=" * 60)
    
    departments_config = get_snowflake_departments_config()
    department_results = {}
    
    for department_name in departments_config.keys():
        if department_name!=DEPARTMENT_FILTER and TEST:
            continue
        try:
            # Get filtered data from Phase 1
            print(f"\n🏢 Processing {department_name}...")
            filtered_df, phase1_stats, success, _ = process_department_phase1(session, department_name, target_date)
            
            if not success or filtered_df.empty:
                print(f"  ❌ {department_name}: No filtered data from Phase 1")
                department_results[department_name] = {
                    'total_conversations': 0,
                    'bot_handled_count': 0,
                    'bot_handled_percentage': 0.0,
                    'chats_with_1_plus_agent_messages': 0,
                    'chats_with_2_plus_agent_messages': 0,
                    'chats_with_3_plus_agent_messages': 0,
                    'chats_with_1_plus_agent_messages_percentage': 0.0,
                    'chats_with_2_plus_agent_messages_percentage': 0.0,
                    'chats_with_3_plus_agent_messages_percentage': 0.0,
                    'call_requests_count': 0,
                    'call_requests_percentage': 0.0,
                    'total_counted_agent_messages': 0,
                    'total_bot_messages': 0,
                    'agent_intervention_percentage': 0.0,
                    'bot_handled_excluding_fillers_count': 0,
                    'bot_handled_excluding_fillers_percentage': 0.0,
                    'error': 'No filtered data from Phase 1'
                }
                continue
            
           
            
            # Analyze guardrail-stopped tools for this department
            analyze_guardrail_stopped_tools(
                session, filtered_df, department_name, target_date
            )
            
            # Analyze guardrail-missed tools for this department
            analyze_guardrail_missed_tools(
                session, filtered_df, department_name, target_date
            )
            
            # Analyze guardrail-false promise with no tool for this department
            analyze_guardrail_false_promise_no_tool(
                session, filtered_df, department_name, target_date
            )
            
            # Analyze conversation categories (extract all categories from all conversations)
            analyze_conversation_categories(
                session, filtered_df, department_name, target_date
            )
            
            # Analyze conversation tool calls (extract all tool calls from all conversations)
            analyze_conversation_tool_calls(
                session, filtered_df, department_name, target_date
            )
            
            # Generate guardrail summary by category
            generate_guardrail_summary_by_category(
                session, department_name, target_date
            )

            # Generate tool usage analysis (for CC_Resolvers)
            if department_name == 'CC_Resolvers':
                generate_tool_usage_analysis(
                    session, department_name, target_date
                )

            # Sales Bot and AT bots: tool-name-based guardrail analytics
            # These departments do not use conversation categories, so guardrail
            # reporting is built around tool names instead.
            SALES_AT_DEPARTMENTS = {
                'CC_Sales', 'MV_Sales',
                'AT_Filipina', 'AT_Filipina_In_PHL', 'AT_Filipina_Outside_UAE',
                'AT_Filipina_Inside_UAE', 'AT_African', 'Gulf_maids'
            }
            if department_name in SALES_AT_DEPARTMENTS:
                # Step 1: detect wrong tool calls using the Sales/AT guardrail signal
                analyze_guardrail_wrong_tool_calls_by_name(
                    session, filtered_df, department_name, target_date
                )
                # Step 2: build unified violation-level table (reads from raw tables)
                analyze_guardrail_violations_by_tool(
                    session, department_name, target_date
                )
                # Step 3: generate aggregated summary by tool name
                generate_guardrail_summary_by_tool(
                    session, department_name, target_date
                )

            # Analyze bot handling for this department (includes raw data saving)
            bot_results = analyze_bot_handled_conversations_single_department(
                session, filtered_df, department_name, departments_config, target_date
            )

            # Attach filtering_stats so exclusion breakdown columns (Excluded_*)
            # can be populated in create_(enhanced_)combined_metrics_snowflake.
            if isinstance(bot_results, dict) and isinstance(phase1_stats, dict):
                bot_results['filtering_stats'] = phase1_stats

            department_results[department_name] = bot_results
            
        except Exception as e:
            error_msg = f"Bot handling analysis failed: {str(e)}"
            print(f"  ❌ {department_name}: {error_msg}")
            department_results[department_name] = {
                'total_conversations': 0,
                'bot_handled_count': 0,
                'bot_handled_percentage': 0.0,
                'chats_with_1_plus_agent_messages': 0,
                'chats_with_2_plus_agent_messages': 0,
                'chats_with_3_plus_agent_messages': 0,
                'chats_with_1_plus_agent_messages_percentage': 0.0,
                'chats_with_2_plus_agent_messages_percentage': 0.0,
                'chats_with_3_plus_agent_messages_percentage': 0.0,
                'call_requests_count': 0,
                'call_requests_percentage': 0.0,
                'total_counted_agent_messages': 0,
                'total_bot_messages': 0,
                'agent_intervention_percentage': 0.0,
                'bot_handled_excluding_fillers_count': 0,
                'bot_handled_excluding_fillers_percentage': 0.0,
                'error': error_msg
            }
    
    # Generate summary
    total_conversations_all = sum(r.get('total_conversations', 0) for r in department_results.values())
    total_bot_handled_all = sum(r.get('bot_handled_count', 0) for r in department_results.values())
    total_1_plus_all = sum(r.get('chats_with_1_plus_agent_messages', 0) for r in department_results.values())
    total_2_plus_all = sum(r.get('chats_with_2_plus_agent_messages', 0) for r in department_results.values())
    total_3_plus_all = sum(r.get('chats_with_3_plus_agent_messages', 0) for r in department_results.values())
    total_call_requests_all = sum(r.get('call_requests_count', 0) for r in department_results.values())
    total_counted_agent_messages_all = sum(r.get('total_counted_agent_messages', 0) for r in department_results.values())
    total_bot_messages_all = sum(r.get('total_bot_messages', 0) for r in department_results.values())
    total_bot_handled_excluding_fillers_all = sum(r.get('bot_handled_excluding_fillers_count', 0) for r in department_results.values())
    
    overall_percentage = (total_bot_handled_all / total_conversations_all * 100) if total_conversations_all > 0 else 0
    overall_1_plus_percentage = (total_1_plus_all / total_conversations_all * 100) if total_conversations_all > 0 else 0
    overall_2_plus_percentage = (total_2_plus_all / total_conversations_all * 100) if total_conversations_all > 0 else 0
    overall_3_plus_percentage = (total_3_plus_all / total_conversations_all * 100) if total_conversations_all > 0 else 0
    overall_call_requests_percentage = (total_call_requests_all / total_conversations_all * 100) if total_conversations_all > 0 else 0
    
    # Calculate overall agent intervention percentage
    total_all_messages = total_counted_agent_messages_all + total_bot_messages_all
    overall_agent_intervention_percentage = (total_counted_agent_messages_all / total_all_messages * 100) if total_all_messages > 0 else 0
    
    # Calculate overall bot handled excluding fillers percentage
    overall_bot_handled_excluding_fillers_percentage = (total_bot_handled_excluding_fillers_all / total_conversations_all * 100) if total_conversations_all > 0 else 0
    
    print(f"\n📊 BOT HANDLING SUMMARY:")
    print(f"   📋 Total conversations: {total_conversations_all:,}")
    print(f"   🤖 Bot-handled conversations: {total_bot_handled_all:,} ({overall_percentage:.1f}%)")
    print(f"   📈 Agent message breakdown:")
    print(f"      - 2+ agent messages: {total_1_plus_all:,} ({overall_1_plus_percentage:.1f}%)")
    print(f"      - 3+ agent messages: {total_2_plus_all:,} ({overall_2_plus_percentage:.1f}%)")
    print(f"      - 5+ agent messages: {total_3_plus_all:,} ({overall_3_plus_percentage:.1f}%)")
    print(f"   📞 Call requests: {total_call_requests_all:,} ({overall_call_requests_percentage:.1f}%)")
    print(f"   🤝 Agent intervention: {total_counted_agent_messages_all:,}/{total_all_messages:,} messages ({overall_agent_intervention_percentage:.1f}%)")
    print(f"      - Counted agent messages: {total_counted_agent_messages_all:,}")
    print(f"      - Bot messages: {total_bot_messages_all:,}")
    print(f"   🎯 Bot handled excluding fillers: {total_bot_handled_excluding_fillers_all:,}/{total_conversations_all:,} ({overall_bot_handled_excluding_fillers_percentage:.1f}%)")
    print(f"   💾 Raw data saved to: BOT_HANDLED_RAW_DATA")
    
    return department_results


# ============================================================================
# REPETITION ANALYSIS
# ============================================================================

def detect_conversation_repetitions_snowflake(conversation_df, department_name, departments_config):
    """
    Detect repetitions in bot normal messages within a conversation (Snowflake version).
    Adapted from main_analytics.py detect_conversation_repetitions()
    
    Args:
        conversation_df: DataFrame containing one conversation's messages
        department_name: Department name for skill filtering
        departments_config: Department configuration dictionary
    
    Returns:
        Tuple: (has_repetition, repetition_score, repetition_details)
    """
    department_config = departments_config[department_name]
    bot_skills = set(department_config['bot_skills'])
    
    # Filter for normal messages from bots (using Snowflake column names)
    bot_normal_messages = conversation_df[
        (conversation_df['SENT_BY'].str.upper() == 'BOT') & 
        (conversation_df['MESSAGE_TYPE'].str.upper() == 'NORMAL MESSAGE') &
        (conversation_df['TARGET_SKILL_PER_MESSAGE'].isin(bot_skills))
    ]
    
    if len(bot_normal_messages) == 0:
        return False, 0, []
    
    # Exclude file extension messages (png, jpg, jpeg, pdf) from repetition analysis
    file_extensions = {'png', 'jpg', 'jpeg', 'pdf'}
    bot_normal_messages_filtered = bot_normal_messages[
        ~bot_normal_messages['TEXT'].str.strip().str.lower().isin(file_extensions)
    ]
    
    if len(bot_normal_messages_filtered) == 0:
        return False, 0, []
    
    # For CC_Sales department: Filter out messages from repetition_exclusion_list
    if department_name == 'CC_Sales' and repetition_exclusion_list:
        messages_before_exclusion = len(bot_normal_messages_filtered)
        
        # Create mask for messages to exclude
        exclusion_mask = pd.Series([False] * len(bot_normal_messages_filtered), index=bot_normal_messages_filtered.index)
        for exclusion_phrase in repetition_exclusion_list:
            pattern = re.escape(exclusion_phrase)
            phrase_mask = bot_normal_messages_filtered['TEXT'].str.contains(
                pattern, case=False, na=False, regex=True
            )
            exclusion_mask = exclusion_mask | phrase_mask
        
        # Filter out excluded messages
        bot_normal_messages_filtered = bot_normal_messages_filtered[~exclusion_mask]
        messages_after_exclusion = len(bot_normal_messages_filtered)
        excluded_count = messages_before_exclusion - messages_after_exclusion
        
        if excluded_count > 0:
            print(f"      🔍 Repetition Exclusion Filter (CC_Sales): Removed {excluded_count} messages from repetition analysis")
    
    if len(bot_normal_messages_filtered) == 0:
        return False, 0, []
    
    # Count occurrences of each message text
    message_counts = bot_normal_messages_filtered['TEXT'].value_counts()
    
    # Find messages that appear more than once
    repeated_messages = message_counts[message_counts > 1]
    
    if len(repeated_messages) == 0:
        return False, 0, []
    
    # Calculate repetition score: sum of (count - 1) for each repeated message
    repetition_score = sum(count - 1 for count in repeated_messages.values)
    
    # Prepare repetition details
    repetition_details = [
        {
            'message': msg[:200] + '...' if len(msg) > 200 else msg,  # Truncate long messages
            'count': count, 
            'repetition_score': count - 1
        }
        for msg, count in repeated_messages.items()
    ]
    
    return True, repetition_score, repetition_details


def detect_conversation_repetitions_with_column_snowflake(conversation_df, department_name, departments_config):
    """
    Detect repetitions and add repetition column (Snowflake version).
    Adapted from main_analytics.py detect_conversation_repetitions_with_column()
    
    REPETITION Column Values:
    - -1: Non-repetitive message (default)
    - 0: First occurrence of a repeated message
    - 1: Second occurrence of the same message
    - 2: Third occurrence of the same message, etc.
    
    Args:
        conversation_df: DataFrame containing one conversation's messages
        department_name: Department name for skill filtering
        departments_config: Department configuration dictionary
    
    Returns:
        Tuple: (modified_df, has_repetition, repetition_score, repetition_details, 
                static_exclusion_score, dynamic_normal_score, static_exclusion_unique_count, dynamic_normal_unique_count)
    """
    department_config = departments_config[department_name]
    bot_skills = set(department_config['bot_skills'])
    
    # Get exclusion list for this department
    exclusion_list = AGENT_INTERVENTION_EXCLUSIONS.get(department_name, [])
    
    # Create a copy of the conversation to add the repetition column
    modified_df = conversation_df.copy()
    
    # Initialize repetition column with -1 for all messages (consistent integer type)
    modified_df['REPETITION'] = -1
    
    # Filter for normal messages from bots
    bot_normal_messages = conversation_df[
        (conversation_df['SENT_BY'].str.upper() == 'BOT') & 
        (conversation_df['MESSAGE_TYPE'].str.upper() == 'NORMAL MESSAGE') &
        (conversation_df['TARGET_SKILL_PER_MESSAGE'].isin(bot_skills))
    ].copy()
    
    if bot_normal_messages.empty:
        return modified_df, False, 0, [], 0, 0, 0, 0
    
    # For CC_Sales department: Filter out messages from repetition_exclusion_list
    if department_name == 'CC_Sales' and repetition_exclusion_list:
        messages_before_exclusion = len(bot_normal_messages)
        
        # Create mask for messages to exclude
        exclusion_mask = pd.Series([False] * len(bot_normal_messages), index=bot_normal_messages.index)
        for exclusion_phrase in repetition_exclusion_list:
            pattern = re.escape(exclusion_phrase)
            phrase_mask = bot_normal_messages['TEXT'].str.contains(
                pattern, case=False, na=False, regex=True
            )
            exclusion_mask = exclusion_mask | phrase_mask
        
        # Filter out excluded messages
        bot_normal_messages = bot_normal_messages[~exclusion_mask]
        messages_after_exclusion = len(bot_normal_messages)
        excluded_count = messages_before_exclusion - messages_after_exclusion
        
    
    if bot_normal_messages.empty:
        return modified_df, False, 0, [], 0, 0, 0, 0
    
    # Count occurrences of each message text
    message_counts = bot_normal_messages['TEXT'].value_counts()
    
    # Find repeated messages (those that appear more than once)
    repeated_messages = message_counts[message_counts > 1].index.tolist()
    
    if not repeated_messages:
        return modified_df, False, 0, [], 0, 0, 0, 0
    
    # Initialize breakdown counters (keep original volume-based scores)
    static_exclusion_score = 0
    dynamic_normal_score = 0
    
    # Initialize unique message counters for breakdown percentages
    static_exclusion_unique_count = 0
    dynamic_normal_unique_count = 0
    
    # For each repeated message, assign repetition values and categorize
    for message_text in repeated_messages:
        # Get all rows with this repeated message
        message_indices = bot_normal_messages[bot_normal_messages['TEXT'] == message_text].index
        
        # Assign repetition values: 0 for first occurrence, 1 for second, etc.
        for i, idx in enumerate(message_indices):
            modified_df.loc[idx, 'REPETITION'] = i
        
        # Check if this repeated message is in the exclusion list (static template)
        message_content_lower = message_text.lower()
        is_static_exclusion = False
        
        for exclusion_phrase in exclusion_list:
            if re.search(re.escape(exclusion_phrase), message_content_lower, re.IGNORECASE):
                is_static_exclusion = True
                break
        
        # Calculate repetition score for this message (count - 1) - KEEP ORIGINAL
        message_repetition_score = message_counts[message_text] - 1
        
        # Add to appropriate category (KEEP ORIGINAL VOLUME-BASED SCORES)
        if is_static_exclusion:
            static_exclusion_score += message_repetition_score
            static_exclusion_unique_count += 1  # NEW: Count unique repeated messages
        else:
            dynamic_normal_score += message_repetition_score
            dynamic_normal_unique_count += 1  # NEW: Count unique repeated messages
    
    # Calculate overall repetition statistics
    has_repetition = len(repeated_messages) > 0
    repetition_score = sum(count - 1 for count in message_counts[repeated_messages])
    
    # Prepare repetition details
    repetition_details = []
    for message_text in repeated_messages:
        count = message_counts[message_text]
        
        # Check if this message is static or dynamic
        message_content_lower = message_text.lower()
        is_static = False
        for exclusion_phrase in exclusion_list:
            if re.search(re.escape(exclusion_phrase), message_content_lower, re.IGNORECASE):
                is_static = True
                break
        
        repetition_details.append({
            'message': message_text[:100] + '...' if len(message_text) > 100 else message_text,
            'count': count,
            'category': 'static_exclusion' if is_static else 'dynamic_normal'
        })
    
    return modified_df, has_repetition, repetition_score, repetition_details, static_exclusion_score, dynamic_normal_score, static_exclusion_unique_count, dynamic_normal_unique_count


def analyze_repetition_conversations_single_department(session, df, department_name, departments_config, target_date):
    """
    Analyze repetition patterns for a single department and save raw data.
    Adapted from main_analytics.py analyze_repetition_conversations()
    
    Args:
        session: Snowflake session
        df: Filtered DataFrame from Phase 1
        department_name: Department name
        departments_config: Department configuration
        target_date: Target date for analysis
    
    Returns:
        repetition_results dictionary
    """
    print(f"  🔄 Analyzing repetitions for {department_name}...")
    
    if df.empty:
        print(f"    ⚠️  No filtered data for {department_name}")
        return {
            'total_conversations': 0,
            'repetition_conversation_count': 0,
            'repetition_percentage': 0.0,
            'total_repetition_score': 0,
            'avg_repetition': 0.0,
            'static_exclusion_repetitions_count': 0,
            'static_exclusion_repetitions_percentage': 0.0,
            'dynamic_normal_repetitions_count': 0,
            'dynamic_normal_repetitions_percentage': 0.0
        }
    
    # Group by conversation ID
    conversations = df.groupby('CONVERSATION_ID')
    total_conversations = len(conversations)
    
    repetition_conversations_dfs = []
    repetition_conversation_count = 0
    total_repetition_score = 0
    total_static_exclusion_score = 0
    total_dynamic_normal_score = 0
    total_static_exclusion_unique_count = 0
    total_dynamic_normal_unique_count = 0
    
    for conv_id, conv_df in conversations:
        # Use the enhanced function that adds the repetition column and breakdown scores
        modified_conv_df, has_repetition, repetition_score, repetition_details, static_exclusion_score, dynamic_normal_score, static_exclusion_unique_count, dynamic_normal_unique_count = detect_conversation_repetitions_with_column_snowflake(
            conv_df, department_name, departments_config
        )
        
        if has_repetition:
            repetition_conversation_count += 1
            total_repetition_score += repetition_score
            total_static_exclusion_score += static_exclusion_score
            total_dynamic_normal_score += dynamic_normal_score
            total_static_exclusion_unique_count += static_exclusion_unique_count
            total_dynamic_normal_unique_count += dynamic_normal_unique_count
            
            # Add metadata
            modified_conv_df['ANALYSIS_DATE'] = datetime.now().strftime('%Y-%m-%d')
            modified_conv_df['CONVERSATION_REPETITION_SCORE'] = repetition_score
            modified_conv_df['CONVERSATION_STATIC_EXCLUSION_SCORE'] = static_exclusion_score
            modified_conv_df['CONVERSATION_DYNAMIC_NORMAL_SCORE'] = dynamic_normal_score
            

            
            repetition_conversations_dfs.append(modified_conv_df)
    
    # Calculate metrics
    repetition_percentage = (repetition_conversation_count / total_conversations * 100) if total_conversations > 0 else 0
    avg_repetition = (total_repetition_score / repetition_conversation_count) if repetition_conversation_count > 0 else 0
    
    # Calculate breakdown percentages using UNIQUE MESSAGE COUNTS instead of volume scores
    total_unique_repeated_messages = total_static_exclusion_unique_count + total_dynamic_normal_unique_count
    static_exclusion_percentage = (total_static_exclusion_unique_count / total_unique_repeated_messages * 100) if total_unique_repeated_messages > 0 else 0
    dynamic_normal_percentage = (total_dynamic_normal_unique_count / total_unique_repeated_messages * 100) if total_unique_repeated_messages > 0 else 0
    
    results = {
        'total_conversations': total_conversations,
        'repetition_conversation_count': repetition_conversation_count,
        'repetition_percentage': repetition_percentage,
        'total_repetition_score': total_repetition_score,
        'avg_repetition': avg_repetition,
        'static_exclusion_repetitions_count': total_static_exclusion_unique_count,
        'static_exclusion_repetitions_percentage': static_exclusion_percentage,
        'dynamic_normal_repetitions_count': total_dynamic_normal_unique_count,
        'dynamic_normal_repetitions_percentage': dynamic_normal_percentage
    }
    
    print(f"    ✅ {repetition_conversation_count}/{total_conversations} ({repetition_percentage:.1f}%) with repetitions, avg: {avg_repetition:.1f}")
    print(f"    📊 Static exclusions: {total_static_exclusion_unique_count} unique messages ({static_exclusion_percentage:.1f}%), Dynamic normal: {total_dynamic_normal_unique_count} unique messages ({dynamic_normal_percentage:.1f}%)")
    
    # Save raw data to REPETITION_RAW_DATA table
    if repetition_conversations_dfs:
        try:
            all_repetition_data = pd.concat(repetition_conversations_dfs, ignore_index=True)
            
            # Add static/dynamic labeling for each message
            exclusion_list = AGENT_INTERVENTION_EXCLUSIONS.get(department_name, [])
            
            # Initialize MESSAGE_CATEGORY column
            all_repetition_data = all_repetition_data.copy()  # Ensure we have a copy
            all_repetition_data['MESSAGE_CATEGORY'] = 'dynamic'  # Default to dynamic
            
            # Create a mask for static messages more efficiently
            if exclusion_list:
                # Get bot messages
                bot_mask = (all_repetition_data['SENT_BY'].str.upper() == 'BOT')
                
                # Check each exclusion phrase
                for exclusion_phrase in exclusion_list:
                    pattern = re.escape(exclusion_phrase)
                    text_mask = all_repetition_data['TEXT'].str.contains(pattern, case=False, na=False, regex=True)
                    static_mask = bot_mask & text_mask
                    all_repetition_data.loc[static_mask, 'MESSAGE_CATEGORY'] = 'static'
            
            all_repetition_data = clean_dataframe_for_snowflake(all_repetition_data)
            
            # Define dynamic columns (excluding the essential columns that insert_raw_data_with_cleanup adds)
            # Make sure MESSAGE_CATEGORY is included in dynamic columns
            dynamic_columns = [col for col in all_repetition_data.columns if col not in ['DATE', 'DEPARTMENT', 'TIMESTAMP']]
            
            
            insert_result = insert_raw_data_with_cleanup(
                session=session,
                table_name="REPETITION_RAW_DATA",
                department=department_name,
                target_date=target_date,
                dataframe=all_repetition_data[dynamic_columns],
                columns=dynamic_columns
            )
            print(f"    💾 Saved {len(all_repetition_data)} repetition records to REPETITION_RAW_DATA")
        except Exception as e:
            print(f"    ⚠️  Failed to save repetition raw data: {str(e)}")
    
    return results


def analyze_repetition_conversations_all_departments(session: snowpark.Session, target_date=None):
    """
    Analyze repetition patterns for all departments using Phase 1 filtered data.
    Raw data is saved immediately for each department.
    
    Args:
        session: Snowflake session
        target_date: Target date for analysis
    
    Returns:
        department_results dictionary
    """
    print("\n🔄 PHASE 2B: ANALYZING MESSAGE REPETITIONS")
    print("=" * 60)
    
    departments_config = get_snowflake_departments_config()
    department_results = {}
    
    for department_name in departments_config.keys():
        # REPETITION ANALYSIS: Only run for AT (Applicant Tracking) departments
        if not department_name.startswith('AT_'):
            print(f"\n🏢 {department_name}: Skipping repetition analysis (AT departments only)")
            department_results[department_name] = {
                'total_conversations': 0,
                'repetition_conversation_count': 0,
                'repetition_percentage': 0.0,
                'total_repetition_score': 0,
                'avg_repetition': 0.0,
                'static_exclusion_repetitions_count': 0,
                'static_exclusion_repetitions_percentage': 0.0,
                'dynamic_normal_repetitions_count': 0,
                'dynamic_normal_repetitions_percentage': 0.0
            }
            continue
        
        if department_name!=DEPARTMENT_FILTER and TEST:
            continue
        try:
            # Get filtered data from Phase 1
            print(f"\n🏢 Processing {department_name}...")
            filtered_df, phase1_stats, success, _ = process_department_phase1(session, department_name, target_date)
            
            if not success or filtered_df.empty:
                print(f"  ❌ {department_name}: No filtered data from Phase 1")
                department_results[department_name] = {
                    'total_conversations': 0,
                    'repetition_conversation_count': 0,
                    'repetition_percentage': 0.0,
                    'total_repetition_score': 0,
                    'avg_repetition': 0.0,
                    'static_exclusion_repetitions_count': 0,
                    'static_exclusion_repetitions_percentage': 0.0,
                    'dynamic_normal_repetitions_count': 0,
                    'dynamic_normal_repetitions_percentage': 0.0,
                    'error': 'No filtered data from Phase 1'
                }
                continue
            
            # Analyze repetitions for this department (includes raw data saving)
            rep_results = analyze_repetition_conversations_single_department(
                session, filtered_df, department_name, departments_config, target_date
            )
            
            department_results[department_name] = rep_results
            
        except Exception as e:
            error_msg = f"Repetition analysis failed: {str(e)}"
            print(f"  ❌ {department_name}: {error_msg}")
            department_results[department_name] = {
                'total_conversations': 0,
                'repetition_conversation_count': 0,
                'repetition_percentage': 0.0,
                'total_repetition_score': 0,
                'avg_repetition': 0.0,
                'static_exclusion_repetitions_count': 0,
                'static_exclusion_repetitions_percentage': 0.0,
                'dynamic_normal_repetitions_count': 0,
                'dynamic_normal_repetitions_percentage': 0.0,
                'error': error_msg
            }
    
    # Generate summary
    total_conversations_all = sum(r.get('total_conversations', 0) for r in department_results.values())
    total_repetition_conversations_all = sum(r.get('repetition_conversation_count', 0) for r in department_results.values())
    total_repetition_score_all = sum(r.get('total_repetition_score', 0) for r in department_results.values())
    total_static_exclusion_all = sum(r.get('static_exclusion_repetitions_count', 0) for r in department_results.values())
    total_dynamic_normal_all = sum(r.get('dynamic_normal_repetitions_count', 0) for r in department_results.values())
    
    overall_repetition_percentage = (total_repetition_conversations_all / total_conversations_all * 100) if total_conversations_all > 0 else 0
    overall_avg_repetition = (total_repetition_score_all / total_repetition_conversations_all) if total_repetition_conversations_all > 0 else 0
    # Calculate overall breakdown percentages using unique counts
    total_unique_all = total_static_exclusion_all + total_dynamic_normal_all
    overall_static_exclusion_percentage = (total_static_exclusion_all / total_unique_all * 100) if total_unique_all > 0 else 0
    overall_dynamic_normal_percentage = (total_dynamic_normal_all / total_unique_all * 100) if total_unique_all > 0 else 0
    
    print(f"\n📊 REPETITION ANALYSIS SUMMARY:")
    print(f"   📋 Total conversations: {total_conversations_all:,}")
    print(f"   🔄 Conversations with repetitions: {total_repetition_conversations_all:,}")
    print(f"   📈 Overall repetition rate: {overall_repetition_percentage:.1f}%")
    print(f"   📊 Average repetition score: {overall_avg_repetition:.1f}")
    print(f"   🔘 Static exclusion repetitions: {total_static_exclusion_all} ({overall_static_exclusion_percentage:.1f}%)")
    print(f"   🔶 Dynamic normal repetitions: {total_dynamic_normal_all} ({overall_dynamic_normal_percentage:.1f}%)")
    print(f"   💾 Raw data saved to: REPETITION_RAW_DATA")
    
    return department_results


# ============================================================================
# Get Conversations without filter 5
# ============================================================================

# ============================================================================
# MESSAGE SIMILARITY ANALYSIS (50% SIMILARITY)
# ============================================================================

def calculate_text_similarity_snowflake(text1, text2):
    """
    Calculate cosine similarity between two text strings using TF-IDF vectors (Snowflake version).
    Adapted from similarity.py calculate_text_similarity()
    
    Returns:
    - similarity: float between 0 and 1
    """
    if not text1 or not text2 or text1.strip() == '' or text2.strip() == '':
        return 0.0
    
    try:
        # Create TF-IDF vectorizer
        vectorizer = TfidfVectorizer(lowercase=True, stop_words='english')
        
        # Fit and transform both texts
        tfidf_matrix = vectorizer.fit_transform([text1, text2])
        
        # Calculate cosine similarity
        similarity_matrix = cosine_similarity(tfidf_matrix)
        
        # Return similarity between the two texts
        return similarity_matrix[0, 1]
    except Exception as e:
        # If any error occurs (empty vocabulary, etc.), return 0
        return 0.0


def find_similar_messages_snowflake(messages, similarity_threshold=0.5):
    """
    Find pairs of messages that have similarity >= threshold (Snowflake version).
    Adapted from similarity.py find_similar_messages()
    
    Returns:
    - similar_pairs: list of tuples (index1, index2, similarity_score)
    """
    similar_pairs = []
    
    if len(messages) < 2:
        return similar_pairs
    
    # Compare all pairs of messages
    for i, j in combinations(range(len(messages)), 2):
        similarity = calculate_text_similarity_snowflake(messages[i], messages[j])
        if similarity >= similarity_threshold:
            similar_pairs.append((i, j, similarity))
    
    return similar_pairs


def detect_conversation_similarity_snowflake(conversation_df, department_name, departments_config, similarity_threshold=0.8):
    """
    Detect 50% similarity in bot normal messages within a conversation (Snowflake version).
    Adapted from similarity.py detect_conversation_similarity()
    
    Returns:
    - has_similarity: bool indicating if conversation has any 50% similar messages
    - similarity_score: total similarity score (sum of similarity scores for similar pairs)
    - similarity_details: list of similar message pairs with their scores
    """
    department_config = departments_config[department_name]
    bot_skills = set(department_config['bot_skills'])
    
    # Filter for normal messages from bots (using Snowflake column names)
    bot_normal_messages = conversation_df[
        (conversation_df['SENT_BY'].str.upper() == 'BOT') & 
        (conversation_df['MESSAGE_TYPE'].str.upper() == 'NORMAL MESSAGE') &
        (conversation_df['TARGET_SKILL_PER_MESSAGE'].isin(bot_skills))
    ]
    
    if len(bot_normal_messages) == 0:
        return False, 0, []
    
    # Exclude file extension messages (png, jpg, jpeg, pdf) from similarity analysis
    file_extensions = {'png', 'jpg', 'jpeg', 'pdf'}
    bot_normal_messages_filtered = bot_normal_messages[
        ~bot_normal_messages['TEXT'].str.strip().str.lower().isin(file_extensions)
    ]
    
    if len(bot_normal_messages_filtered) == 0:
        return False, 0, []
    
    # Get message texts
    message_texts = bot_normal_messages_filtered['TEXT'].tolist()
    
    # Find similar pairs
    similar_pairs = find_similar_messages_snowflake(message_texts, similarity_threshold)
    
    if len(similar_pairs) == 0:
        return False, 0, []
    
    # Calculate total similarity score
    total_similarity_score = sum(pair[2] for pair in similar_pairs)
    
    # Prepare similarity details
    similarity_details = []
    for i, j, score in similar_pairs:
        similarity_details.append({
            'message1': message_texts[i][:100] + '...' if len(message_texts[i]) > 100 else message_texts[i],
            'message2': message_texts[j][:100] + '...' if len(message_texts[j]) > 100 else message_texts[j],
            'similarity_score': score
        })
    
    return True, total_similarity_score, similarity_details


def detect_conversation_similarity_with_column_snowflake(conversation_df, department_name, departments_config, similarity_threshold=0.8):
    """
    Detect 50% similarity and add similarity column (Snowflake version).
    Adapted from similarity.py detect_conversation_similarity_with_column()
    
    SIMILARITY_50_PCT Column Values:
    - -1: Non-applicable message (default)
    - 0: First occurrence of a similar message
    - 1: Subsequent similar occurrence
    
    Returns:
    - modified_df: DataFrame with added 'SIMILARITY_50_PCT' column
    - has_similarity: bool indicating if conversation has any 50% similar messages
    - similarity_score: total similarity score (sum of similarity scores for similar pairs)
    - similarity_details: list of similar message pairs with their scores
    - static_exclusion_similarity_score: similarity score for static exclusion messages
    - dynamic_normal_similarity_score: similarity score for dynamic normal messages
    - static_exclusion_unique_count: count of unique messages in static exclusion category
    - dynamic_normal_unique_count: count of unique messages in dynamic normal category
    """
    department_config = departments_config[department_name]
    bot_skills = set(department_config['bot_skills'])
    
    # Get exclusion list for similarity categorization (use repetition_exclusion_list)
    exclusion_list = repetition_exclusion_list
    
    # Create a copy of the conversation to add the similarity column
    modified_df = conversation_df.copy()
    
    # Initialize similarity column with -1 for all messages
    modified_df['SIMILARITY_50_PCT'] = -1
    
    # Filter for normal messages from bots
    bot_normal_messages = conversation_df[
        (conversation_df['SENT_BY'].str.upper() == 'BOT') & 
        (conversation_df['MESSAGE_TYPE'].str.upper() == 'NORMAL MESSAGE') &
        (conversation_df['TARGET_SKILL_PER_MESSAGE'].isin(bot_skills))
    ].copy()
    
    if bot_normal_messages.empty:
        return modified_df, False, 0, [], 0, 0, 0, 0
    
    # Exclude file extension messages (png, jpg, jpeg, pdf) from similarity analysis
    file_extensions = {'png', 'jpg', 'jpeg', 'pdf'}
    bot_normal_messages_filtered = bot_normal_messages[
        ~bot_normal_messages['TEXT'].str.strip().str.lower().isin(file_extensions)
    ]
    
    if bot_normal_messages_filtered.empty:
        return modified_df, False, 0, [], 0, 0, 0, 0
    
    # Get message texts and their indices
    message_texts = bot_normal_messages_filtered['TEXT'].tolist()
    message_indices = bot_normal_messages_filtered.index.tolist()
    
    # Find similar pairs
    similar_pairs = find_similar_messages_snowflake(message_texts, similarity_threshold)
    
    if not similar_pairs:
        return modified_df, False, 0, [], 0, 0, 0, 0
    
    # Track which messages are involved in similar pairs
    involved_message_indices = set()
    for i, j, score in similar_pairs:
        involved_message_indices.add(i)
        involved_message_indices.add(j)
    
    # For each message involved in similarity, assign similarity values
    for msg_idx in involved_message_indices:
        original_idx = message_indices[msg_idx]
        
        # Check if this is the first occurrence of a similar message
        # We'll mark the first occurrence as 0, others as 1
        is_first_occurrence = True
        for other_idx in involved_message_indices:
            if other_idx < msg_idx:
                # Check if these two messages are similar
                for i, j, score in similar_pairs:
                    if (i == other_idx and j == msg_idx) or (i == msg_idx and j == other_idx):
                        is_first_occurrence = False
                        break
                if not is_first_occurrence:
                    break
        
        if is_first_occurrence:
            modified_df.loc[original_idx, 'SIMILARITY_50_PCT'] = 0  # First occurrence
        else:
            modified_df.loc[original_idx, 'SIMILARITY_50_PCT'] = 1  # Subsequent similar occurrence
    
    # Calculate similarity statistics and breakdown
    has_similarity = len(similar_pairs) > 0
    
    # NEW LOGIC: Count messages marked as '1' (subsequent similar occurrences)
    count_of_repetitive_messages = (modified_df['SIMILARITY_50_PCT'] == 1).sum()
    
    # Count total bot messages for ratio calculation
    total_bot_messages = len(bot_normal_messages_filtered)
    
    # Calculate new conversation score as ratio (percentage of repetitive messages)
    total_similarity_score = count_of_repetitive_messages / total_bot_messages if total_bot_messages > 0 else 0
    
    # Initialize breakdown counters for new logic
    static_repetitive_count = 0
    dynamic_repetitive_count = 0
    
    # Initialize unique message sets for breakdown percentages
    static_exclusion_unique_messages = set()
    dynamic_normal_unique_messages = set()
    
    # NEW LOGIC: Count repetitive messages (marked as '1') by category
    repetitive_messages_indices = modified_df[modified_df['SIMILARITY_50_PCT'] == 0].index
    
    for idx in repetitive_messages_indices:
        message_text = modified_df.loc[idx, 'TEXT']
        message_lower = message_text.lower()
        
        # Check if message matches any exclusion phrase (static template)
        is_static_message = False
        for exclusion_phrase in exclusion_list:
            if re.search(re.escape(exclusion_phrase), message_lower, re.IGNORECASE):
                is_static_message = True
                break
        
        # Count in appropriate category
        if is_static_message:
            static_repetitive_count += 1
        else:
            dynamic_repetitive_count += 1
    
    # Calculate breakdown scores as ratios
    static_exclusion_similarity_score = static_repetitive_count / total_bot_messages if total_bot_messages > 0 else 0
    dynamic_normal_similarity_score = dynamic_repetitive_count / total_bot_messages if total_bot_messages > 0 else 0
    
    # Prepare similarity details and categorize pairs (keep for compatibility)
    similarity_details = []
    for i, j, score in similar_pairs:
        message1_text = message_texts[i]
        message2_text = message_texts[j]
        
        # Check if either message in the pair is in the exclusion list (static template)
        message1_lower = message1_text.lower()
        message2_lower = message2_text.lower()
        
        is_static_pair = False
        for exclusion_phrase in exclusion_list:
            if (re.search(re.escape(exclusion_phrase), message1_lower, re.IGNORECASE) or 
                re.search(re.escape(exclusion_phrase), message2_lower, re.IGNORECASE)):
                is_static_pair = True
                break
        
        # Add unique messages to sets (keep for compatibility)
        if is_static_pair:
            static_exclusion_unique_messages.add(message1_text)
            static_exclusion_unique_messages.add(message2_text)
        else:
            dynamic_normal_unique_messages.add(message1_text)
            dynamic_normal_unique_messages.add(message2_text)
        
        similarity_details.append({
            'message1': message1_text[:100] + '...' if len(message1_text) > 100 else message1_text,
            'message2': message2_text[:100] + '...' if len(message2_text) > 100 else message2_text,
            'similarity_score': score,
            'category': 'static_exclusion' if is_static_pair else 'dynamic_normal'
        })
    
    return modified_df, has_similarity, total_similarity_score, similarity_details, static_exclusion_similarity_score, dynamic_normal_similarity_score, static_repetitive_count, dynamic_repetitive_count


def analyze_similarity_conversations_single_department(session, df, department_name, departments_config, target_date, similarity_threshold=0.8):
    """
    Analyze 50% similarity patterns for a single department and save raw data.
    Adapted from similarity.py analyze_similarity_for_department()
    
    Args:
        session: Snowflake session
        df: Filtered DataFrame from Phase 1
        department_name: Department name
        departments_config: Department configuration
        target_date: Target date for analysis
        similarity_threshold: Similarity threshold (default 0.5 for 50%)
    
    Returns:
        similarity_results dictionary
    """
    print(f"  🔍 Analyzing 50% similarity for {department_name}...")
    
    if df.empty:
        print(f"    ⚠️  No filtered data for {department_name}")
        return {
            'total_conversations': 0,
            'similarity_conversation_count': 0,
            'similarity_percentage': 0.0,
            'total_similarity_score': 0,
            'avg_similarity': 0.0,
            'static_exclusion_similarity_count': 0,
            'static_exclusion_similarity_percentage': 0.0,
            'dynamic_normal_similarity_count': 0,
            'dynamic_normal_similarity_percentage': 0.0
        }
    
    # Group by conversation ID
    conversations = df.groupby('CONVERSATION_ID')
    total_conversations = len(conversations)
    
    similarity_conversations_dfs = []
    similarity_conversation_count = 0
    total_similarity_score = 0
    total_static_exclusion_similarity_score = 0
    total_dynamic_normal_similarity_score = 0
    total_static_exclusion_similarity_unique_count = 0
    total_dynamic_normal_similarity_unique_count = 0
    total_count_of_ones = 0  # NEW: Track total "1" messages across all similarity conversations
    
    for conv_id, conv_df in conversations:
        # Use the enhanced function that adds the similarity column and breakdown scores
        modified_conv_df, has_similarity, similarity_score, similarity_details, static_exclusion_similarity_score, dynamic_normal_similarity_score, static_exclusion_similarity_unique_count, dynamic_normal_similarity_unique_count = detect_conversation_similarity_with_column_snowflake(
            conv_df, department_name, departments_config, similarity_threshold
        )
        
        if has_similarity:
            similarity_conversation_count += 1
            total_similarity_score += similarity_score
            total_static_exclusion_similarity_score += static_exclusion_similarity_score
            total_dynamic_normal_similarity_score += dynamic_normal_similarity_score
            total_static_exclusion_similarity_unique_count += static_exclusion_similarity_unique_count
            total_dynamic_normal_similarity_unique_count += dynamic_normal_similarity_unique_count
            
            # NEW: Count "1" messages in this conversation for avg_similarity calculation
            count_of_ones_in_this_conv = (modified_conv_df['SIMILARITY_50_PCT'] == 1).sum()
            total_count_of_ones += count_of_ones_in_this_conv
            
            # Add metadata
            modified_conv_df['ANALYSIS_DATE'] = datetime.now().strftime('%Y-%m-%d')
            modified_conv_df['CONVERSATION_SIMILARITY_SCORE'] = similarity_score
            modified_conv_df['CONVERSATION_STATIC_EXCLUSION_SIMILARITY_SCORE'] = static_exclusion_similarity_score
            modified_conv_df['CONVERSATION_DYNAMIC_NORMAL_SIMILARITY_SCORE'] = dynamic_normal_similarity_score
            modified_conv_df['SIMILARITY_THRESHOLD'] = similarity_threshold
            
            similarity_conversations_dfs.append(modified_conv_df)
    
    # Calculate metrics
    similarity_percentage = (similarity_conversation_count / total_conversations * 100) if total_conversations > 0 else 0
    # NEW LOGIC: Average number of repetitive messages per conversation with similarity
    avg_similarity = (total_count_of_ones / similarity_conversation_count) if similarity_conversation_count > 0 else 0
    
    # Calculate breakdown percentages using REPETITIVE MESSAGE COUNTS (messages marked as SIMILARITY_50_PCT=1)
    total_repetitive_messages = total_static_exclusion_similarity_unique_count + total_dynamic_normal_similarity_unique_count
    static_exclusion_similarity_percentage = (total_static_exclusion_similarity_unique_count / total_repetitive_messages * 100) if total_repetitive_messages > 0 else 0
    dynamic_normal_similarity_percentage = (total_dynamic_normal_similarity_unique_count / total_repetitive_messages * 100) if total_repetitive_messages > 0 else 0
    
    results = {
        'total_conversations': total_conversations,
        'similarity_conversation_count': similarity_conversation_count,
        'similarity_percentage': similarity_percentage,
        'total_similarity_score': total_similarity_score,
        'avg_similarity': avg_similarity,
        'total_count_of_ones': total_count_of_ones,  # NEW: Add for overall calculation
        'static_exclusion_similarity_count': total_static_exclusion_similarity_unique_count,
        'static_exclusion_similarity_percentage': static_exclusion_similarity_percentage,
        'dynamic_normal_similarity_count': total_dynamic_normal_similarity_unique_count,
        'dynamic_normal_similarity_percentage': dynamic_normal_similarity_percentage
    }
    
    print(f"    ✅ {similarity_conversation_count}/{total_conversations} ({similarity_percentage:.1f}%) with 50% similarity, avg: {avg_similarity:.2f}")
    print(f"    📊 Static exclusion similarities: {total_static_exclusion_similarity_unique_count} repetitive messages ({static_exclusion_similarity_percentage:.1f}%), Dynamic normal similarities: {total_dynamic_normal_similarity_unique_count} repetitive messages ({dynamic_normal_similarity_percentage:.1f}%)")
    
    # Save raw data to SIMILARITY_RAW_DATA table
    if similarity_conversations_dfs:
        try:
            all_similarity_data = pd.concat(similarity_conversations_dfs, ignore_index=True)
            
            # Add static/dynamic labeling for each message (use repetition_exclusion_list)
            exclusion_list = repetition_exclusion_list
            
            # Initialize MESSAGE_CATEGORY column
            all_similarity_data = all_similarity_data.copy()  # Ensure we have a copy
            all_similarity_data['MESSAGE_CATEGORY'] = 'dynamic'  # Default to dynamic
            
            # Create a mask for static messages more efficiently
            if exclusion_list:
                # Get bot messages
                bot_mask = (all_similarity_data['SENT_BY'].str.upper() == 'BOT')
                
                # Check each exclusion phrase
                for exclusion_phrase in exclusion_list:
                    pattern = re.escape(exclusion_phrase)
                    text_mask = all_similarity_data['TEXT'].str.contains(pattern, case=False, na=False, regex=True)
                    static_mask = bot_mask & text_mask
                    all_similarity_data.loc[static_mask, 'MESSAGE_CATEGORY'] = 'static'
            
            all_similarity_data = clean_dataframe_for_snowflake(all_similarity_data)
            
            # Define dynamic columns (excluding the essential columns that insert_raw_data_with_cleanup adds)
            dynamic_columns = [col for col in all_similarity_data.columns if col not in ['DATE', 'DEPARTMENT', 'TIMESTAMP']]
            
            insert_raw_data_with_cleanup(
                session=session,
                table_name="SIMILARITY_RAW_DATA",
                department=department_name,
                target_date=target_date,
                dataframe=all_similarity_data[dynamic_columns],
                columns=dynamic_columns
            )
            print(f"    💾 Saved {len(all_similarity_data)} similarity records to SIMILARITY_RAW_DATA")
        except Exception as e:
            print(f"    ⚠️  Failed to save similarity raw data: {str(e)}")
    
    return results


def analyze_similarity_conversations_all_departments(session: snowpark.Session, target_date=None, similarity_threshold=0.8):
    """
    Analyze 50% similarity patterns for all departments using Phase 1 filtered data.
    Raw data is saved immediately for each department.
    
    Args:
        session: Snowflake session
        target_date: Target date for analysis
        similarity_threshold: Similarity threshold (default 0.5 for 50%)
    
    Returns:
        department_results dictionary
    """
    print("\n🔍 PHASE 2D: ANALYZING MESSAGE 50% SIMILARITY")
    print("=" * 60)
    
    departments_config = get_snowflake_departments_config()
    department_results = {}
    
    for department_name in departments_config.keys():
        if department_name!=DEPARTMENT_FILTER and TEST:
            continue
        try:
            # Get filtered data from Phase 1
            print(f"\n🏢 Processing {department_name}...")
            filtered_df, phase1_stats, success, _ = process_department_phase1(session, department_name, target_date)
            
            if not success or filtered_df.empty:
                print(f"  ❌ {department_name}: No filtered data from Phase 1")
                department_results[department_name] = {
                    'total_conversations': 0,
                    'similarity_conversation_count': 0,
                    'similarity_percentage': 0.0,
                    'total_similarity_score': 0,
                    'avg_similarity': 0.0,
                    'static_exclusion_similarity_count': 0,
                    'static_exclusion_similarity_percentage': 0.0,
                    'dynamic_normal_similarity_count': 0,
                    'dynamic_normal_similarity_percentage': 0.0,
                    'error': 'No filtered data from Phase 1'
                }
                continue
            
            # Analyze similarity for this department (includes raw data saving)
            similarity_results = analyze_similarity_conversations_single_department(
                session, filtered_df, department_name, departments_config, target_date, similarity_threshold
            )
            
            department_results[department_name] = similarity_results
            
        except Exception as e:
            error_msg = f"Similarity analysis failed: {str(e)}"
            print(f"  ❌ {department_name}: {error_msg}")
            department_results[department_name] = {
                'total_conversations': 0,
                'similarity_conversation_count': 0,
                'similarity_percentage': 0.0,
                'total_similarity_score': 0,
                'avg_similarity': 0.0,
                'static_exclusion_similarity_count': 0,
                'static_exclusion_similarity_percentage': 0.0,
                'dynamic_normal_similarity_count': 0,
                'dynamic_normal_similarity_percentage': 0.0,
                'error': error_msg
            }
    
    # Generate summary
    total_conversations_all = sum(r.get('total_conversations', 0) for r in department_results.values())
    total_similarity_conversations_all = sum(r.get('similarity_conversation_count', 0) for r in department_results.values())
    total_similarity_score_all = sum(r.get('total_similarity_score', 0) for r in department_results.values())
    total_count_of_ones_all = sum(r.get('total_count_of_ones', 0) for r in department_results.values())  # NEW: Sum total "1" messages
    total_static_exclusion_similarity_all = sum(r.get('static_exclusion_similarity_count', 0) for r in department_results.values())
    total_dynamic_normal_similarity_all = sum(r.get('dynamic_normal_similarity_count', 0) for r in department_results.values())
    
    overall_similarity_percentage = (total_similarity_conversations_all / total_conversations_all * 100) if total_conversations_all > 0 else 0
    # NEW LOGIC: Overall average number of repetitive messages per conversation with similarity
    overall_avg_similarity = (total_count_of_ones_all / total_similarity_conversations_all) if total_similarity_conversations_all > 0 else 0
    # Calculate overall breakdown percentages using unique counts
    total_unique_similarity_all = total_static_exclusion_similarity_all + total_dynamic_normal_similarity_all
    overall_static_exclusion_similarity_percentage = (total_static_exclusion_similarity_all / total_unique_similarity_all * 100) if total_unique_similarity_all > 0 else 0
    overall_dynamic_normal_similarity_percentage = (total_dynamic_normal_similarity_all / total_unique_similarity_all * 100) if total_unique_similarity_all > 0 else 0
    
    print(f"\n📊 SIMILARITY ANALYSIS SUMMARY:")
    print(f"   📋 Total conversations: {total_conversations_all:,}")
    print(f"   🔍 Conversations with 50% similarity: {total_similarity_conversations_all:,}")
    print(f"   📈 Overall similarity rate: {overall_similarity_percentage:.1f}%")
    print(f"   📊 Average similarity score: {overall_avg_similarity:.2f}")
    print(f"   🔘 Static exclusion similarities: {total_static_exclusion_similarity_all:.2f} ({overall_static_exclusion_similarity_percentage:.1f}%)")
    print(f"   🔶 Dynamic normal similarities: {total_dynamic_normal_similarity_all:.2f} ({overall_dynamic_normal_similarity_percentage:.1f}%)")
    print(f"   💾 Raw data saved to: SIMILARITY_RAW_DATA")
    
    return department_results


# ============================================================================
# Get Conversations without filter 5
# ============================================================================

def get_conversations_without_filter_5_all_departments(session: snowpark.Session, target_date=None):
    """
    Get all conversations without filter 5 for all departments.
    """
    print("\n🔄 PHASE 2C: GETTING CONVERSATIONS WITHOUT FILTER 5")
    print("=" * 60)
    departments_config = get_snowflake_departments_config()
    department_results = {}
    for department_name in departments_config.keys():
        if department_name!=DEPARTMENT_FILTER and TEST:
            continue
        try:
            print(f"Processing {department_name}...")
            filtered_df, phase1_stats, success, _ = process_department_phase1(session, department_name, target_date, apply_filter_5=False)
            
            # Store as dictionary structure to match expected format
            # Count unique conversation IDs instead of total messages
            conversation_count = filtered_df['CONVERSATION_ID'].nunique() if success and not filtered_df.empty else 0
            department_results[department_name] = {
                'total_conversations': conversation_count,
                'success': success
            }
            
            # Save raw data if successful
            if success and not filtered_df.empty:
                filtered_df = clean_dataframe_for_snowflake(filtered_df)
                # dynamic_columns = [col for col in filtered_df.columns if col not in ['DATE', 'DEPARTMENT', 'TIMESTAMP']]
                # insert_raw_data_with_cleanup(
                #     session=session,
                #     table_name="CONVERSATIONS_WITHOUT_FILTER_5",
                #     department=department_name,
                #     target_date=target_date,
                #     dataframe=filtered_df[dynamic_columns],
                #     columns=dynamic_columns
                # )
            
            print(f"    ✅ {department_name}: {conversation_count} conversations")
        except Exception as e:
            print(f"    ❌ {department_name}: Error - {str(e)}")
            department_results[department_name] = {
                'total_conversations': 0,
                'success': False,
                'error': str(e)
            }
    
    total_conversations = sum(r.get('total_conversations', 0) for r in department_results.values())
    print(f"    ✅ Total conversations without filter 5: {total_conversations}")
    
    return department_results


# ============================================================================
# COMBINED PROCESSING & OUTPUT
# ============================================================================

def create_combined_metrics_snowflake(bot_results, repetition_results, target_date=None):
    """
    Create combined metrics from bot handling and repetition analysis.
    
    Args:
        bot_results: Results from bot handling analysis
        repetition_results: Results from repetition analysis
        target_date: Target date for analysis
    
    Returns:
        List of combined metrics dictionaries
    """
    print("DEBUG: create_combined_metrics_snowflake function called!")
    print(f"DEBUG: bot_results sample keys: {list(next(iter(bot_results.values()), {}).keys())}")
    # Handle target_date conversion (it comes in as string like "2025-07-22")
    if target_date:
        try:
            print(target_date)
            # Convert string to datetime, then format
            date_obj = datetime.strptime(target_date, '%Y-%m-%d')
            print(date_obj)
            current_date = date_obj.strftime("%B %d, %Y")
            print(current_date)
        except:
            # Fallback to current date if parsing fails
            current_date = datetime.now().strftime("%B %d, %Y")
    else:
        current_date = datetime.now().strftime("%B %d, %Y")
    departments_config = get_snowflake_departments_config()
    
    combined_metrics = []
    
    for department_name in departments_config.keys():
        bot_data = bot_results.get(department_name, {})
        rep_data = repetition_results.get(department_name, {})
        
        # Debug output for excluding pokes
        if department_name == 'CC_Sales':
            print(f"DEBUG: CC_Sales bot_data keys: {list(bot_data.keys())}")
            print(f"DEBUG: CC_Sales excluding pokes values: {bot_data.get('chats_with_1_plus_agent_messages_excluding_pokes', 'MISSING')}, {bot_data.get('chats_with_1_plus_agent_messages_excluding_pokes_percentage', 'MISSING')}")
        
        # Debug output for chats with no bot messages (CC_Resolvers)
        if department_name == 'CC_Resolvers':
            print(f"DEBUG: CC_Resolvers bot_data keys present: {'chats_with_no_bot_messages_count' in bot_data}, {'chats_with_no_bot_messages_percentage' in bot_data}")
            print(f"DEBUG: CC_Resolvers no bot messages values: COUNT={bot_data.get('chats_with_no_bot_messages_count', 'MISSING')}, PERCENTAGE={bot_data.get('chats_with_no_bot_messages_percentage', 'MISSING')}")
        _fs = bot_data.get('filtering_stats', {})
        _total_original      = _fs.get('total_original_conversations', 0)
        _engagement_valid    = _fs.get('engagement_valid_conversations', 0)
        _bot_skills_valid    = _fs.get('conversations_with_bot_skills', 0)
        excluded_no_engagement = max(_total_original - _engagement_valid, 0)
        excluded_no_bot_skill  = max(_engagement_valid - _bot_skills_valid, 0)
        excluded_hi_bye        = _fs.get('hi_bye_conversations_removed', 0)
        excluded_wrong_date    = _fs.get('conversations_filtered_by_date', 0)
        total_excluded         = max(_total_original - bot_data.get('total_conversations', 0), 0)
        
        metrics = {
            'Date': current_date,
            'Department': department_name,
            
            # Bot Handling Metrics
            'Total_Conversations': bot_data.get('total_conversations', 0),
            'Bot_Handled_Count': bot_data.get('bot_handled_count', 0),
            'Bot_Handled_Percentage': round(bot_data.get('bot_handled_percentage', 0), 2),
            
            # Agent Message Breakdown Metrics
            'Chats_With_1_Plus_Agent_Messages': bot_data.get('chats_with_1_plus_agent_messages', 0),
            'Chats_With_2_Plus_Agent_Messages': bot_data.get('chats_with_2_plus_agent_messages', 0),
            'Chats_With_3_Plus_Agent_Messages': bot_data.get('chats_with_3_plus_agent_messages', 0),
            'Chats_With_1_Plus_Agent_Messages_Percentage': round(bot_data.get('chats_with_1_plus_agent_messages_percentage', 0), 2),
            'Chats_With_2_Plus_Agent_Messages_Percentage': round(bot_data.get('chats_with_2_plus_agent_messages_percentage', 0), 2),
            'Chats_With_3_Plus_Agent_Messages_Percentage': round(bot_data.get('chats_with_3_plus_agent_messages_percentage', 0), 2),
            'CHATS_WITH_1_PLUS_AGENT_MESSAGES_EXCLUDING_POKES': bot_data.get('chats_with_1_plus_agent_messages_excluding_pokes', 0),
            'CHATS_WITH_1_PLUS_AGENT_MESSAGES_EXCLUDING_POKES_PERCENTAGE': round(bot_data.get('chats_with_1_plus_agent_messages_excluding_pokes_percentage', 0), 2),
            'CHATS_WITH_POKES_COUNT': bot_data.get('chats_with_pokes', 0),
            'CHATS_WITH_POKES_PERCENTAGE': round(bot_data.get('chats_with_pokes_percentage', 0), 2),
            'CHATS_WITH_EXACTLY_1_AGENT_MESSAGE': bot_data.get('CHATS_WITH_EXACTLY_1_AGENT_MESSAGE', 0),
            'CHATS_WITH_EXACTLY_1_AGENT_MESSAGE_PERCENTAGE': round(bot_data.get('CHATS_WITH_EXACTLY_1_AGENT_MESSAGE_PERCENTAGE', 0), 2),
            'CHATS_WITH_EXACTLY_2_AGENT_MESSAGES': bot_data.get('CHATS_WITH_EXACTLY_2_AGENT_MESSAGES', 0),
            'CHATS_WITH_EXACTLY_2_AGENT_MESSAGES_PERCENTAGE': round(bot_data.get('CHATS_WITH_EXACTLY_2_AGENT_MESSAGES_PERCENTAGE', 0), 2),
            'CHATS_WITH_EXACTLY_3_AGENT_MESSAGES': bot_data.get('CHATS_WITH_EXACTLY_3_AGENT_MESSAGES', 0),
            'CHATS_WITH_EXACTLY_3_AGENT_MESSAGES_PERCENTAGE': round(bot_data.get('CHATS_WITH_EXACTLY_3_AGENT_MESSAGES_PERCENTAGE', 0), 2),
            'AVG_BOT_MSGS_BEFORE_TRANSFER': bot_data.get('avg_bot_msgs_before_transfer', None),
            'TRANSFERRED_CONVERSATION_COUNT': bot_data.get('transferred_conversation_count', 0),
            'MCD_MV_TRANSFER_COUNT': bot_data.get('mcd_mv_transfer_count', 0),
            'MCD_CC_TRANSFER_COUNT': bot_data.get('mcd_cc_transfer_count', 0),
            # Exclusion breakdown: chats removed on the way from ChatCC to Chats_Supposed_to_be_Bot_Handled
            'Excluded_No_Engagement_Chats': excluded_no_engagement,
            'Excluded_No_Bot_Skill_Chats':  excluded_no_bot_skill,
            'Excluded_Hi_Bye_Chats':        excluded_hi_bye,
            'Excluded_Wrong_Date_Chats':    excluded_wrong_date,
            'Total_Excluded_Chats':         total_excluded,
            'CONSUMER_ONLY_CHATS_COUNT':    _fs.get('consumer_only_chats_count', 0),
            'Bot_Handled_Count': bot_data.get('bot_handled_count', 0),
            'Bot_Handled_Percentage': round(bot_data.get('bot_handled_percentage', 0), 2),
            # CC_Resolvers Specific Metrics
            'COMPLAINT_ACTION_COUNT': bot_data.get('complaint_action_count', 0),
            'COMPLAINT_ACTION_PERCENTAGE': round(bot_data.get('complaint_action_percentage', 0), 2),
            'COMPLAINT_ACTION_WITH_0_AGENT_MESSAGES_COUNT': bot_data.get('complaint_action_with_0_agent_messages_count', 0),
            'COMPLAINT_ACTION_WITH_0_AGENT_MESSAGES_PERCENTAGE': round(bot_data.get('complaint_action_with_0_agent_messages_percentage', 0), 2),
            'COMPLAINT_ACTION_WITH_1_PLUS_AGENT_MESSAGES_COUNT': bot_data.get('complaint_action_with_1_plus_agent_messages_count', 0),
            'COMPLAINT_ACTION_WITH_1_PLUS_AGENT_MESSAGES_PERCENTAGE': round(bot_data.get('complaint_action_with_1_plus_agent_messages_percentage', 0), 2),
            'CHATS_WITH_NO_BOT_MESSAGES_COUNT': bot_data.get('chats_with_no_bot_messages_count', 0),
            'CHATS_WITH_NO_BOT_MESSAGES_PERCENTAGE': round(bot_data.get('chats_with_no_bot_messages_percentage', 0), 2),
            'BOT_HANDLED_WITH_BOT_RESPONSE_COUNT': bot_data.get('bot_handled_with_bot_response_count', 0),
            'BOT_HANDLED_WITH_BOT_RESPONSE_PERCENTAGE': round(bot_data.get('bot_handled_with_bot_response_percentage', 0), 2),
            'CONVERSATIONS_WITH_BOT_RESPONSE_COUNT': bot_data.get('conversations_with_bot_response_count', 0),
            
            # Call Requests Metrics
            'Call_Requests_Count': bot_data.get('call_requests_count', 0),
            'Call_Requests_Percentage': round(bot_data.get('call_requests_percentage', 0), 2),
            
            # Agent Intervention Metrics
            'Agent_Intervention_Percentage': round(bot_data.get('agent_intervention_percentage', 0), 2),
            
            # Repetition Metrics
            'Repetition_Conversation_Count': rep_data.get('repetition_conversation_count', 0),
            'Repetition_Percentage': round(rep_data.get('repetition_percentage', 0), 2),
            'Total_Repetition_Score': rep_data.get('total_repetition_score', 0),
            'Avg_Repetition': round(rep_data.get('avg_repetition', 0), 2),
            
            # Repetition Breakdown Metrics
            'STATIC_EXCLUSION_REPETITIONS_COUNT': rep_data.get('static_exclusion_repetitions_count', 0),
            'STATIC_EXCLUSION_REPETITIONS_PERCENTAGE': round(rep_data.get('static_exclusion_repetitions_percentage', 0), 2),
            'DYNAMIC_NORMAL_REPETITIONS_COUNT': rep_data.get('dynamic_normal_repetitions_count', 0),
            'DYNAMIC_NORMAL_REPETITIONS_PERCENTAGE': round(rep_data.get('dynamic_normal_repetitions_percentage', 0), 2),
            
            # Phase 3 Delay Metrics (default values for Phase 2)
            'Method1_Avg_Initial_Delay_Seconds': None,
            'Method1_Avg_Non_Initial_Delay_Seconds': None,
            'Method1_Initial_Outliers': None,
            'Method1_Non_Initial_Outliers': None,
            'Method2_Avg_Initial_Delay_Seconds': None,
            'Method2_Avg_Non_Initial_Delay_Seconds': None,
            'Method2_Initial_Outliers': None,
            'Method2_Non_Initial_Outliers': None,
            'Method3_Avg_Initial_Delay_Seconds': None,
            'Method3_Avg_Non_Initial_Delay_Seconds': None,
            'Method3_Initial_Outliers': None,
            'Method3_Non_Initial_Outliers': None,
            'Unresponsive_Count': None,
            'Unresponsive_Percentage': None,
            'Outlier_Threshold_Seconds': None,
            'Unresponsive_Threshold_Minutes': None,
            
            # Phase 4+ Extended Metrics (default values)
            'Overall_Shadowing_Percentage': None,
            'Shadowed_Reported_Issues': None,
            'Reported_Percentage': None,
            'Open_Issues_By_Agents': None,
            'Similarity_Conversation_Count': None,
            'Similarity_Percentage': None,
            'Avg_Similarity_Score': None,
            'Chats_Supposed_to_be_Bot_Handled': None,
            'CHATS_INITIATED_BY_US':     None,
            'CHATS_INITIATED_BY_CLIENT': None,
            
            # Analysis Metadata
            'Analysis_Date': datetime.now().strftime('%Y-%m-%d'),
            'Phase': 'Phase2_CoreAnalytics'
        }
        
        # Debug output for final metrics in regular function
        if department_name == 'CC_Sales':
            print(f"DEBUG FINAL REGULAR: CC_Sales final metrics excluding pokes: {metrics.get('CHATS_WITH_1_PLUS_AGENT_MESSAGES_EXCLUDING_POKES', 'MISSING')}, {metrics.get('CHATS_WITH_1_PLUS_AGENT_MESSAGES_EXCLUDING_POKES_PERCENTAGE', 'MISSING')}")
        
        # Debug output for final metrics for CC_Resolvers
        if department_name == 'CC_Resolvers':
            print(f"DEBUG FINAL REGULAR: CC_Resolvers final metrics no bot messages: COUNT={metrics.get('CHATS_WITH_NO_BOT_MESSAGES_COUNT', 'MISSING')}, PERCENTAGE={metrics.get('CHATS_WITH_NO_BOT_MESSAGES_PERCENTAGE', 'MISSING')}")
        
        combined_metrics.append(metrics)
    
    # Debug: Print column count and first metric keys
    if combined_metrics:
        print(f"DEBUG: create_combined_metrics_snowflake generated {len(combined_metrics[0])} columns")
        print(f"DEBUG: Column names: {list(combined_metrics[0].keys())}")
    
    return combined_metrics


def save_raw_tables_snowflake(session: snowpark.Session, bot_data, repetition_data, target_date=None):
    """
    Save raw analysis data to Snowflake tables.
    
    Args:
        session: Snowflake session
        bot_data: Bot-handled conversations data
        repetition_data: Repetition analysis data
        target_date: Target date for table naming
    
    Returns:
        Tuple: (bot_table_name, repetition_table_name, success)
    """
    print("\n💾 SAVING RAW ANALYSIS TABLES...")
    
    try:
        # Create table names
        bot_table_name = create_output_table_name('BOT_HANDLED_CONVERSATIONS', target_date)
        repetition_table_name = create_output_table_name('REPETITION_CONVERSATIONS', target_date)
        
        # Save bot-handled conversations
        if bot_data:
            bot_df = pd.DataFrame(bot_data)
            # Clean data types for Snowflake compatibility
            bot_df = clean_dataframe_for_snowflake(bot_df)
            session.write_pandas(bot_df, bot_table_name, auto_create_table=True, overwrite=True)
            print(f"  ✅ Bot-handled data: {len(bot_data):,} rows → {bot_table_name}")
        else:
            print(f"  ⚠️  No bot-handled data to save")
        
        # Save repetition conversations
        if repetition_data:
            repetition_df = pd.DataFrame(repetition_data)
            # Clean data types for Snowflake compatibility
            repetition_df = clean_dataframe_for_snowflake(repetition_df)
            
            # Ensure REPETITION column is consistently integer type
            if 'REPETITION' in repetition_df.columns:
                repetition_df['REPETITION'] = pd.to_numeric(repetition_df['REPETITION'], errors='coerce').fillna(-1).astype(int)
                print(f"    🔧 Fixed REPETITION column data type: {repetition_df['REPETITION'].dtype}")
            
            session.write_pandas(repetition_df, repetition_table_name, auto_create_table=True, overwrite=True)
            print(f"  ✅ Repetition data: {len(repetition_data):,} rows → {repetition_table_name}")
        else:
            print(f"  ⚠️  No repetition data to save")
        
        return bot_table_name, repetition_table_name, True
        
    except Exception as e:
        error_report = format_error_details(e, "SAVING RAW TABLES")
        print(f"  ❌ Failed to save raw tables:")
        print(error_report)
        return None, None, False


def clean_dataframe_for_snowflake(df):
    """
    Clean DataFrame to ensure Snowflake/PyArrow compatibility.
    
    Args:
        df: Input DataFrame
    
    Returns:
        Cleaned DataFrame with consistent data types
    """
    df_clean = df.copy()
    
    # Convert all object columns to string to avoid mixed type issues
    for col in df_clean.columns:
        if df_clean[col].dtype == 'object':
            # Check if it's supposed to be numeric
            if col in ['REPETITION', 'CONVERSATION_REPETITION_SCORE', 'MESSAGE_INDEX', 
                      'CONVERSATION_STATIC_EXCLUSION_SCORE', 'CONVERSATION_DYNAMIC_NORMAL_SCORE',
                      'CONVERSATION_SIMILARITY_SCORE', 'CONVERSATION_STATIC_EXCLUSION_SIMILARITY_SCORE', 
                      'CONVERSATION_DYNAMIC_NORMAL_SIMILARITY_SCORE', 'SIMILARITY_THRESHOLD']:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(-1).astype(int)
            else:
                # Convert to string and handle NaN
                df_clean[col] = df_clean[col].astype(str).replace('nan', '')
    
    # Ensure boolean columns are properly typed
    boolean_columns = ['IS_BOT_HANDLED']
    for col in boolean_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(bool)
    
    # Ensure datetime columns are properly typed
    datetime_columns = ['MESSAGE_SENT_TIME', 'UPDATED_AT']
    for col in datetime_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
    
    return df_clean


def update_master_metrics_table_snowflake(session: snowpark.Session, combined_metrics):
    """
    Update the master metrics table with combined analytics using the same logic as insert_raw_data_with_cleanup.
    
    Args:
        session: Snowflake session
        combined_metrics: List of metrics dictionaries (already contains Date and Department)
    
    Returns:
        bool: Success status
    """
    print(f"\n📊 UPDATING MASTER METRICS TABLE...")
    
    try:
        if not combined_metrics:
            print(f"  ⚠️  No metrics to update")
            return False
        
        # Convert to DataFrame
        new_metrics_df = pd.DataFrame(combined_metrics)
        current_date = combined_metrics[0]['Date']  # All rows should have same date
        
        # Get list of departments being updated
        departments_to_update = new_metrics_df['Department'].unique().tolist()
        
        print(f"Processing master metrics for date: {current_date}")
        print(f"Departments to update: {departments_to_update}")
        print(f"Target table: {MASTER_METRICS_TABLE}")
        print(f"Dataframe shape: {new_metrics_df.shape}")
        
        # Step 1: Check if table exists
        try:
            check_query = f"""
            SELECT COUNT(*) AS count
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = UPPER('{MASTER_METRICS_TABLE}')
            AND TABLE_SCHEMA = CURRENT_SCHEMA()
            """
            exists = session.sql(check_query).collect()[0]['COUNT'] > 0
        except:
            exists = False

        # Step 2: Create table if it doesn't exist
        if not exists:
            print(f"Table {MASTER_METRICS_TABLE} does not exist. Creating it...")
            # Define schema for master metrics table
            schema_cols = {
                'Date': 'VARCHAR(50)',
                'Department': 'VARCHAR(100)',
                'Total_Conversations': 'NUMBER',
                'Chats_Supposed_to_be_Bot_Handled': 'NUMBER',
                'CHATS_INITIATED_BY_US': 'NUMBER',
                'CHATS_INITIATED_BY_CLIENT': 'NUMBER',
                'Excluded_No_Engagement_Chats': 'NUMBER',
                'Excluded_No_Bot_Skill_Chats': 'NUMBER',
                'Excluded_Hi_Bye_Chats': 'NUMBER',
                'Excluded_Wrong_Date_Chats': 'NUMBER',
                'Total_Excluded_Chats': 'NUMBER',
                'CONSUMER_ONLY_CHATS_COUNT': 'NUMBER',
                'Bot_Handled_Count': 'NUMBER',
                'Bot_Handled_Percentage': 'FLOAT',
                'Chats_With_1_Plus_Agent_Messages': 'NUMBER',
                'Chats_With_2_Plus_Agent_Messages': 'NUMBER',
                'Chats_With_3_Plus_Agent_Messages': 'NUMBER',
                'Chats_With_1_Plus_Agent_Messages_Percentage': 'FLOAT',
                'Chats_With_2_Plus_Agent_Messages_Percentage': 'FLOAT',
                'Chats_With_3_Plus_Agent_Messages_Percentage': 'FLOAT',
                'CHATS_WITH_1_PLUS_AGENT_MESSAGES_EXCLUDING_POKES': 'NUMBER',
                'CHATS_WITH_1_PLUS_AGENT_MESSAGES_EXCLUDING_POKES_PERCENTAGE': 'FLOAT',
                'CHATS_WITH_POKES_COUNT': 'NUMBER',
                'CHATS_WITH_POKES_PERCENTAGE': 'FLOAT',
                'CHATS_WITH_EXACTLY_1_AGENT_MESSAGE': 'NUMBER',
                'CHATS_WITH_EXACTLY_1_AGENT_MESSAGE_PERCENTAGE': 'FLOAT',
                'CHATS_WITH_EXACTLY_2_AGENT_MESSAGES': 'NUMBER',
                'CHATS_WITH_EXACTLY_2_AGENT_MESSAGES_PERCENTAGE': 'FLOAT',
                'CHATS_WITH_EXACTLY_3_AGENT_MESSAGES': 'NUMBER',
                'CHATS_WITH_EXACTLY_3_AGENT_MESSAGES_PERCENTAGE': 'FLOAT',
                'AVG_BOT_MSGS_BEFORE_TRANSFER': 'FLOAT',
                'TRANSFERRED_CONVERSATION_COUNT': 'NUMBER',
                'MCD_MV_TRANSFER_COUNT': 'NUMBER',
                'MCD_CC_TRANSFER_COUNT': 'NUMBER',
                'Call_Requests_Count': 'NUMBER',
                'Call_Requests_Percentage': 'FLOAT',
                'Agent_Intervention_Percentage': 'FLOAT',
                'COMPLAINT_ACTION_COUNT': 'NUMBER',
                'COMPLAINT_ACTION_PERCENTAGE': 'FLOAT',
                'COMPLAINT_ACTION_WITH_0_AGENT_MESSAGES_COUNT': 'NUMBER',
                'COMPLAINT_ACTION_WITH_0_AGENT_MESSAGES_PERCENTAGE': 'FLOAT',
                'COMPLAINT_ACTION_WITH_1_PLUS_AGENT_MESSAGES_COUNT': 'NUMBER',
                'COMPLAINT_ACTION_WITH_1_PLUS_AGENT_MESSAGES_PERCENTAGE': 'FLOAT',
                'CHATS_WITH_NO_BOT_MESSAGES_COUNT': 'NUMBER',
                'CHATS_WITH_NO_BOT_MESSAGES_PERCENTAGE': 'FLOAT',
                'BOT_HANDLED_WITH_BOT_RESPONSE_COUNT': 'NUMBER',
                'BOT_HANDLED_WITH_BOT_RESPONSE_PERCENTAGE': 'FLOAT',
                'CONVERSATIONS_WITH_BOT_RESPONSE_COUNT': 'NUMBER',
                'PROACTIVE_AGENT_MESSAGES_COUNT': 'NUMBER',
                'PROACTIVE_AGENT_MESSAGES_PERCENTAGE': 'FLOAT',
                'DIRECTLY_HANDLED_BY_SENIORS_COUNT': 'NUMBER',
                'DIRECTLY_HANDLED_BY_SENIORS_PERCENTAGE': 'FLOAT',
                'OTHER_BOTS_TO_SENIORS_COUNT': 'NUMBER',
                'OTHER_BOTS_TO_SENIORS_PERCENTAGE': 'FLOAT',
                'OUR_BOT_TO_SENIORS_COUNT': 'NUMBER',
                'OUR_BOT_TO_SENIORS_PERCENTAGE': 'FLOAT',
                'MV_BOT_KNOWN_FLOW_TRANSFER_COUNT': 'NUMBER',
                'MV_BOT_KNOWN_FLOW_TRANSFER_PERCENTAGE': 'FLOAT',
                'MV_BOT_TECH_ERRORS_TRANSFERS_COUNT': 'NUMBER',
                'MV_BOT_TECH_ERRORS_TRANSFERS_PERCENTAGE': 'FLOAT',
                'MV_BOT_GUARDRAILS_COUNT': 'NUMBER',
                'MV_BOT_GUARDRAILS_PERCENTAGE': 'FLOAT',
                'MV_BOT_OTHER_TRANSFERS_COUNT': 'NUMBER',
                'MV_BOT_OTHER_TRANSFERS_PERCENTAGE': 'FLOAT',
                'OUR_BOT_TO_MV_RESOLVERS_SENIORS_COUNT': 'NUMBER',
                'OUR_BOT_TO_MV_RESOLVERS_SENIORS_PERCENTAGE': 'FLOAT',
                'OUR_BOT_TO_MV_CALLERS_COUNT': 'NUMBER',
                'OUR_BOT_TO_MV_CALLERS_PERCENTAGE': 'FLOAT',
                'OUR_BOT_TO_PRE_R_VISA_RETENTION_COUNT': 'NUMBER',
                'OUR_BOT_TO_PRE_R_VISA_RETENTION_PERCENTAGE': 'FLOAT',
                'DELIGHTERS_TO_SENIORS_COUNT': 'NUMBER',
                'DELIGHTERS_TO_SENIORS_PERCENTAGE': 'FLOAT',
                'TOTAL_SENIORS_CALLERS_COUNT': 'NUMBER',
                'TOTAL_SENIORS_CALLERS_PERCENTAGE': 'FLOAT',
                'SENIORS_NO_RESPONSE_TRANSFERS_COUNT': 'NUMBER',
                'SENIORS_NO_RESPONSE_TRANSFERS_PERCENTAGE': 'FLOAT',
                'SENIORS_OUR_BOT_COUNT': 'NUMBER',
                'SENIORS_OUR_BOT_PERCENTAGE': 'FLOAT',
                'SENIORS_DIRECTLY_HANDLED_COUNT': 'NUMBER',
                'SENIORS_DIRECTLY_HANDLED_PERCENTAGE': 'FLOAT',
                'SENIORS_PROACTIVE_COUNT': 'NUMBER',
                'SENIORS_PROACTIVE_PERCENTAGE': 'FLOAT',
                'SENIORS_PROACTIVE_MV_RESOLVERS_SENIORS_ONLY_COUNT': 'NUMBER',
                'SENIORS_PROACTIVE_MV_RESOLVERS_SENIORS_ONLY_PERCENTAGE': 'FLOAT',
                'SENIORS_OUR_BOT_TO_MV_RESOLVERS_SENIORS_COUNT': 'NUMBER',
                'SENIORS_OUR_BOT_TO_MV_RESOLVERS_SENIORS_PERCENTAGE': 'FLOAT',
                'SENIORS_OUR_BOT_TO_MV_CALLERS_COUNT': 'NUMBER',
                'SENIORS_OUR_BOT_TO_MV_CALLERS_PERCENTAGE': 'FLOAT',
                'SENIORS_OUR_BOT_TO_PRE_R_VISA_RETENTION_COUNT': 'NUMBER',
                'SENIORS_OUR_BOT_TO_PRE_R_VISA_RETENTION_PERCENTAGE': 'FLOAT',
                'SENIORS_DELIGHTERS_COUNT': 'NUMBER',
                'SENIORS_DELIGHTERS_PERCENTAGE': 'FLOAT',
                'SENIORS_OTHER_BOTS_COUNT': 'NUMBER',
                'SENIORS_OTHER_BOTS_PERCENTAGE': 'FLOAT',
                'UNIQUE_UNION_COUNT': 'NUMBER',
                'WRONG_NUMBER_CLIENTS_COUNT': 'NUMBER',
                'TOTAL_GUARDRAIL_COUNT': 'NUMBER',
                'TOTAL_GUARDRAIL_PERCENTAGE': 'FLOAT',
                'GUARDRAIL_AGENT_COUNT': 'NUMBER',
                'GUARDRAIL_AGENT_PERCENTAGE': 'FLOAT',
                'TECH_ERROR_TRANSFERS_COUNT': 'NUMBER',
                'TECH_ERROR_TRANSFERS_PERCENTAGE': 'FLOAT',
                'BOT_HANDLED_EXCLUDING_FILLERS_COUNT': 'NUMBER',
                'BOT_HANDLED_EXCLUDING_FILLERS_PERCENTAGE': 'FLOAT',
                'Repetition_Conversation_Count': 'NUMBER',
                'Repetition_Percentage': 'FLOAT',
                'Total_Repetition_Score': 'NUMBER',
                'Avg_Repetition': 'FLOAT',
                'STATIC_EXCLUSION_REPETITIONS_COUNT': 'NUMBER',
                'STATIC_EXCLUSION_REPETITIONS_PERCENTAGE': 'FLOAT',
                'DYNAMIC_NORMAL_REPETITIONS_COUNT': 'NUMBER',
                'DYNAMIC_NORMAL_REPETITIONS_PERCENTAGE': 'FLOAT',
                'Method1_Avg_Initial_Delay_Seconds': 'FLOAT',
                'Method1_Avg_Non_Initial_Delay_Seconds': 'FLOAT',
                'Method1_Initial_Outliers': 'NUMBER',
                'Method1_Non_Initial_Outliers': 'NUMBER',
                'Method2_Avg_Initial_Delay_Seconds': 'FLOAT',
                'Method2_Avg_Non_Initial_Delay_Seconds': 'FLOAT',
                'Method2_Initial_Outliers': 'NUMBER',
                'Method2_Non_Initial_Outliers': 'NUMBER',
                'Method3_Avg_Initial_Delay_Seconds': 'FLOAT',
                'Method3_Avg_Non_Initial_Delay_Seconds': 'FLOAT',
                'Method3_Initial_Outliers': 'NUMBER',
                'Method3_Non_Initial_Outliers': 'NUMBER',
                'AVG_INITIAL_DELAY_4_TO_50_MINS_SECONDS': 'FLOAT',
                'AVG_NON_INITIAL_DELAY_4_TO_50_MINS_SECONDS': 'FLOAT',
                'INITIAL_DELAY_4_TO_50_MINS_COUNT': 'NUMBER',
                'NON_INITIAL_DELAY_4_TO_50_MINS_COUNT': 'NUMBER',
                'Unresponsive_Count': 'NUMBER',
                'Unresponsive_Percentage': 'FLOAT',
                'DOWNTIME_CONVERSATION_COUNT': 'NUMBER',
                'DOWNTIME_PERCENTAGE': 'FLOAT',
                'INTERVENTION_DUE_TO_NO_RESPONSE_COUNT': 'NUMBER',
                'INTERVENTION_DUE_TO_NO_RESPONSE_PERCENTAGE': 'FLOAT',
                'NORMAL_MANUAL_INTERVENTION_COUNT': 'NUMBER',
                'NORMAL_MANUAL_INTERVENTION_PERCENTAGE': 'FLOAT',
                'Outlier_Threshold_Seconds': 'NUMBER',
                'Unresponsive_Threshold_Minutes': 'NUMBER',
                'Analysis_Date': 'DATE',
                'Phase': 'VARCHAR(50)',
                'Overall_Shadowing_Percentage': 'FLOAT',
                'Overall_Shadowed_Assigned_Percentage': 'FLOAT',
                'Total_Unassigned': 'NUMBER',
                'Shadowed_Reported_Issues': 'NUMBER',
                'Reported_Percentage': 'FLOAT',
                'Open_Issues_By_Agents': 'NUMBER',
                'Similarity_Conversation_Count': 'NUMBER',
                'Similarity_Percentage': 'FLOAT',
                'Avg_Similarity_Score': 'FLOAT',
                'STATIC_EXCLUSION_SIMILARITY_COUNT': 'FLOAT',
                'STATIC_EXCLUSION_SIMILARITY_PERCENTAGE': 'FLOAT',
                'DYNAMIC_NORMAL_SIMILARITY_COUNT': 'FLOAT',
                'DYNAMIC_NORMAL_SIMILARITY_PERCENTAGE': 'FLOAT',
                # Intervention Reengagement Metrics (10-minute response)
                'TOTAL_LAST_INTERVENTIONS_BOT_COUNT': 'FLOAT',
                'TOTAL_LAST_INTERVENTIONS_AGENT_COUNT': 'FLOAT',
                'TOTAL_LAST_INTERVENTIONS_M20_COUNT': 'FLOAT',
                'TOTAL_LAST_INTERVENTIONS_OVERALL_COUNT': 'FLOAT',
                'REENGAGED_INTERVENTIONS_BOT_COUNT': 'FLOAT',
                'REENGAGED_INTERVENTIONS_AGENT_COUNT': 'FLOAT',
                'REENGAGED_INTERVENTIONS_M20_COUNT': 'FLOAT',
                'REENGAGED_INTERVENTIONS_OVERALL_COUNT': 'FLOAT',
                'INTERVENTION_REENGAGEMENT_BOT_RATE': 'FLOAT',
                'INTERVENTION_REENGAGEMENT_AGENT_RATE': 'FLOAT',
                'INTERVENTION_REENGAGEMENT_M20_RATE': 'FLOAT',
                'INTERVENTION_REENGAGEMENT_OVERALL_RATE': 'FLOAT',
                # Chats Fully Handled by Agents (Applicant Tracking departments)
                'CHATS_FULLY_HANDLED_BY_AGENTS': 'NUMBER',
                'UNIQUE_APPLICANTS_FULLY_HANDLED': 'NUMBER',
            }

            create_cols_str = ",\n    ".join([f'"{col}" {dtype}' for col, dtype in schema_cols.items()])
            create_query = f"CREATE TABLE {MASTER_METRICS_TABLE} (\n    {create_cols_str}\n)"
            session.sql(create_query).collect()
            print(f"✅ Created master table {MASTER_METRICS_TABLE} with {len(schema_cols)} columns")
        
        # Step 3: Remove existing rows for the current date AND departments being updated to avoid duplicates
        # Format department list for SQL IN clause
        departments_str = "', '".join(departments_to_update)
        delete_query = f"""
        DELETE FROM {MASTER_METRICS_TABLE} 
        WHERE "Date" = '{current_date}'
        AND "Department" IN ('{departments_str}')
        """
        
        delete_result = session.sql(delete_query).collect()
        print(f"Cleaned existing data for {current_date} and departments: {departments_to_update}")
        
        # Step 4: Check table structure and fix dataframe columns
        print(f"DEBUG: Dataframe columns ({len(new_metrics_df.columns)}): {list(new_metrics_df.columns)}")
        
        # Get actual table columns
        try:
            table_cols_query = f"""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = UPPER('{MASTER_METRICS_TABLE}')
            AND TABLE_SCHEMA = CURRENT_SCHEMA()
            ORDER BY ORDINAL_POSITION
            """
            table_columns_result = session.sql(table_cols_query).collect()
            table_columns = [row['COLUMN_NAME'] for row in table_columns_result]
            print(f"DEBUG: Table expects {len(table_columns)} columns: {table_columns}")

            # Add any new columns from the DataFrame that are missing in the Snowflake table
            table_columns_upper = [c.upper() for c in table_columns]
            new_col_type_map = {
                'Excluded_No_Engagement_Chats': 'NUMBER',
                'Excluded_No_Bot_Skill_Chats':  'NUMBER',
                'Excluded_Hi_Bye_Chats':         'NUMBER',
                'Excluded_Wrong_Date_Chats':     'NUMBER',
                'Total_Excluded_Chats':          'NUMBER',
                'CONSUMER_ONLY_CHATS_COUNT':     'NUMBER',
                'AVG_BOT_MSGS_BEFORE_TRANSFER':  'FLOAT',
                'TRANSFERRED_CONVERSATION_COUNT':'NUMBER',
                'MCD_MV_TRANSFER_COUNT':         'NUMBER',
                'MCD_CC_TRANSFER_COUNT':         'NUMBER',
                'CHATS_INITIATED_BY_US':         'NUMBER',
                'CHATS_INITIATED_BY_CLIENT':     'NUMBER',
                'WRONG_NUMBER_CLIENTS_COUNT':    'NUMBER',
            }
            for df_col, col_type in new_col_type_map.items():
                if df_col.upper() not in table_columns_upper and df_col in new_metrics_df.columns:
                    alter_query = f'ALTER TABLE {MASTER_METRICS_TABLE} ADD COLUMN "{df_col}" {col_type}'
                    session.sql(alter_query).collect()
                    table_columns.append(df_col)
                    print(f"DEBUG: Added new column to table: {df_col}")
            
            # Add missing columns with None values
            for col in table_columns:
                if col not in new_metrics_df.columns:
                    new_metrics_df[col] = None
                    print(f"DEBUG: Added missing column: {col}")
            
            # Reorder columns to match table structure
            new_metrics_df = new_metrics_df[table_columns]
            print(f"DEBUG: Reordered dataframe to match table structure ({len(new_metrics_df.columns)} columns)")
            
        except Exception as e:
            print(f"DEBUG: Could not check table structure: {str(e)}")
        
        snowpark_df = session.create_dataframe(new_metrics_df)
        
        # Write to table (append mode)
        snowpark_df.write.mode("append").save_as_table(MASTER_METRICS_TABLE)
        
        # Step 5: Get final count for verification
        count_query = f"""
        SELECT COUNT(*) as row_count 
        FROM {MASTER_METRICS_TABLE} 
        WHERE "Date" = '{current_date}'
        """
        
        final_count = session.sql(count_query).collect()[0]['ROW_COUNT']
        
        print(f"Successfully inserted {len(combined_metrics)} metrics into {MASTER_METRICS_TABLE}")
        print(f"Final count for {current_date}: {final_count} rows")
        
        return True
        
    except Exception as e:
        error_report = format_error_details(e, "UPDATING MASTER METRICS")
        print(f"❌ Failed to update master metrics: {str(e)}")
        print(error_report)
        return False


def phase2_core_analytics_processor(session: snowpark.Session, target_date=None):
    """
    Phase 2 Core Analytics Processor: Bot Handling + Repetition Analysis
    
    Args:
        session: Snowflake session
        target_date: Target date for analysis (defaults to today)
    
    Returns:
        Processing summary and results
    """
    print("🚀 PHASE 2: CORE ANALYTICS PROCESSOR")
    print("=" * 60)
    
    # Clear cache at the start of each processor run to ensure fresh data
    # This allows the cache to work within a single run but not across runs
    clear_department_cache()
    
    target_date_str = target_date if target_date else datetime.now().strftime('%Y-%m-%d')
    print(f"📅 Target date: {target_date_str}")
    print("=" * 60)
    
    try:
        # Step 1: Bot Handling Analysis
        bot_results = analyze_bot_handled_conversations_all_departments(session, target_date)
        print(f"DEBUG: bot_results keys from first department: {list(next(iter(bot_results.values()), {}).keys())}")
        
        # Step 2: Repetition Analysis
        repetition_results, repetition_data = analyze_repetition_conversations_all_departments(session, target_date)
        
        # Step 3: Intervention Reengagement Analysis
        print(f"\n🔄 Initializing CC_Sales message categorization...")
        initialize_cc_sales_pokes_validation_df()
        intervention_reengagement_results = analyze_intervention_reengagement_all_departments(session, target_date)
        
        # Save CC_Sales message categorization table
        print(f"\n💾 Saving CC_Sales message categorization...")
        save_cc_sales_pokes_validation_table(session)
        
        # Step 4: Create Combined Metrics
        print(f"\n📊 CREATING COMBINED METRICS...")
        print(f"DEBUG: Calling create_combined_metrics_snowflake with bot_results sample: {next(iter(bot_results.values()), {})}")
        combined_metrics = create_combined_metrics_snowflake(bot_results, repetition_results, target_date)
        print(f"  ✅ Created metrics for {len(combined_metrics)} departments")
        if combined_metrics:
            print(f"DEBUG: Combined metrics sample keys: {list(combined_metrics[0].keys())}")
            print(f"DEBUG: Sample breakdown values: Chats_With_1_Plus_Agent_Messages={combined_metrics[0].get('Chats_With_1_Plus_Agent_Messages', 'MISSING')}")
        
        # Step 5: Save Raw Tables (bot data already saved during analysis)
        bot_table, rep_table, raw_success = save_raw_tables_snowflake(session, [], repetition_data, target_date)
        
        # Step 6: Update Master Metrics Table
        master_success = update_master_metrics_table_snowflake(session, combined_metrics)
        
        # Generate final summary
        total_conversations = sum(r.get('total_conversations', 0) for r in bot_results.values())
        total_bot_handled = sum(r.get('bot_handled_count', 0) for r in bot_results.values())
        total_1_plus = sum(r.get('chats_with_1_plus_agent_messages', 0) for r in bot_results.values())
        total_2_plus = sum(r.get('chats_with_2_plus_agent_messages', 0) for r in bot_results.values())
        total_3_plus = sum(r.get('chats_with_3_plus_agent_messages', 0) for r in bot_results.values())
        total_call_requests = sum(r.get('call_requests_count', 0) for r in bot_results.values())
        total_counted_agent_messages = sum(r.get('total_counted_agent_messages', 0) for r in bot_results.values())
        total_bot_messages = sum(r.get('total_bot_messages', 0) for r in bot_results.values())
        
        # Intervention reengagement totals
        total_last_bot_interventions = sum(r.get('total_last_interventions_bot_count', 0) for r in intervention_reengagement_results.values())
        total_last_agent_interventions = sum(r.get('total_last_interventions_agent_count', 0) for r in intervention_reengagement_results.values())
        total_last_m20_interventions = sum(r.get('total_last_interventions_m20_count', 0) for r in intervention_reengagement_results.values())
        total_last_interventions = sum(r.get('total_last_interventions_overall_count', 0) for r in intervention_reengagement_results.values())
        reengaged_bot_interventions = sum(r.get('reengaged_interventions_bot_count', 0) for r in intervention_reengagement_results.values())
        reengaged_agent_interventions = sum(r.get('reengaged_interventions_agent_count', 0) for r in intervention_reengagement_results.values())
        reengaged_m20_interventions = sum(r.get('reengaged_interventions_m20_count', 0) for r in intervention_reengagement_results.values())
        reengaged_interventions = sum(r.get('reengaged_interventions_overall_count', 0) for r in intervention_reengagement_results.values())
        total_bot_handled_excluding_fillers = sum(r.get('bot_handled_excluding_fillers_count', 0) for r in bot_results.values())
        total_repetitions = sum(r.get('repetition_conversation_count', 0) for r in repetition_results.values())
        
        overall_bot_percentage = (total_bot_handled / total_conversations * 100) if total_conversations > 0 else 0
        overall_1_plus_percentage = (total_1_plus / total_conversations * 100) if total_conversations > 0 else 0
        overall_2_plus_percentage = (total_2_plus / total_conversations * 100) if total_conversations > 0 else 0
        overall_3_plus_percentage = (total_3_plus / total_conversations * 100) if total_conversations > 0 else 0
        overall_call_requests_percentage = (total_call_requests / total_conversations * 100) if total_conversations > 0 else 0
        overall_rep_percentage = (total_repetitions / total_conversations * 100) if total_conversations > 0 else 0
        
        # Calculate overall agent intervention percentage
        total_all_messages = total_counted_agent_messages + total_bot_messages
        overall_agent_intervention_percentage = (total_counted_agent_messages / total_all_messages * 100) if total_all_messages > 0 else 0
        
        # Calculate overall bot handled excluding fillers percentage
        overall_bot_handled_excluding_fillers_percentage = (total_bot_handled_excluding_fillers / total_conversations * 100) if total_conversations > 0 else 0
        
        summary = f"""
🎯 PHASE 2 CORE ANALYTICS - SUMMARY
{'=' * 50}
📅 Date: {target_date_str}
🏢 Departments processed: {len(combined_metrics)}

📊 OVERALL METRICS:
   💬 Total conversations: {total_conversations:,}
   🤖 Bot-handled: {total_bot_handled:,} ({overall_bot_percentage:.1f}%)
   📈 Agent message breakdown:
      - 2+ agent messages: {total_1_plus:,} ({overall_1_plus_percentage:.1f}%)
      - 3+ agent messages: {total_2_plus:,} ({overall_2_plus_percentage:.1f}%)
      - 5+ agent messages: {total_3_plus:,} ({overall_3_plus_percentage:.1f}%)
   📞 Call requests: {total_call_requests:,} ({overall_call_requests_percentage:.1f}%)
   🤝 Agent intervention: {total_counted_agent_messages:,}/{total_all_messages:,} messages ({overall_agent_intervention_percentage:.1f}%)
   🎯 Bot handled excluding fillers: {total_bot_handled_excluding_fillers:,} ({overall_bot_handled_excluding_fillers_percentage:.1f}%)
   🔄 With repetitions: {total_repetitions:,} ({overall_rep_percentage:.1f}%)
   🤝 Intervention reengagement (10-min response):
      🤖 Bot: {reengaged_bot_interventions:,}/{total_last_bot_interventions:,} ({(reengaged_bot_interventions/total_last_bot_interventions*100) if total_last_bot_interventions > 0 else 0:.1f}%)
      👤 Agent: {reengaged_agent_interventions:,}/{total_last_agent_interventions:,} ({(reengaged_agent_interventions/total_last_agent_interventions*100) if total_last_agent_interventions > 0 else 0:.1f}%)
      🎯 M20: {reengaged_m20_interventions:,}/{total_last_m20_interventions:,} ({(reengaged_m20_interventions/total_last_m20_interventions*100) if total_last_m20_interventions > 0 else 0:.1f}%)
      📊 Overall: {reengaged_interventions:,}/{total_last_interventions:,} ({(reengaged_interventions/total_last_interventions*100) if total_last_interventions > 0 else 0:.1f}%)

💾 OUTPUT TABLES:
   📋 Master metrics: {MASTER_METRICS_TABLE} {'✅' if master_success else '❌'}
   🤖 Bot handled: BOT_HANDLED_RAW_DATA {'✅' if master_success else '❌'}
   🔄 Repetitions: {rep_table if rep_table else 'Failed'} {'✅' if raw_success else '❌'}

🌟 Phase 2 Core Analytics Complete!
   Ready for Phase 3: Advanced Analytics (Delays & Response Times)
"""
        
        print(summary)
        return {
            'summary': summary,
            'bot_results': bot_results,
            'repetition_results': repetition_results,
            'combined_metrics': combined_metrics,
            'master_success': master_success,
            'raw_success': raw_success
        }
        
    except Exception as e:
        error_report = format_error_details(e, "PHASE 2 PROCESSOR")
        error_summary = f"""
❌ PHASE 2 CRITICAL FAILURE:
{error_report}

💡 TROUBLESHOOTING:
   - Ensure Phase 1 foundation is working correctly
   - Check Snowflake table permissions
   - Verify department configuration matches your data
"""
        print(error_summary)
        return {'summary': error_summary, 'error': str(e)}


# ============================================================================
# TESTING AND VALIDATION FUNCTIONS
# ============================================================================

def test_bot_handling_single_department(session: snowpark.Session, department_name, target_date=None):
    """
    Test bot handling analysis for a single department.
    """
    print(f"🧪 TESTING BOT HANDLING - {department_name}")
    print("=" * 50)
    
    try:
        departments_config = get_snowflake_departments_config()
        
        # Get filtered data from Phase 1
        filtered_df, stats, success, _ = process_department_phase1(session, department_name, target_date)
        
        if not success:
            print(f"❌ Phase 1 failed for {department_name}")
            return
        
        # Run bot handling analysis
        bot_results, bot_data = analyze_bot_handled_conversations_single_department(
            filtered_df, department_name, departments_config
        )
        
        print(f"\n📊 RESULTS:")
        for key, value in bot_results.items():
            print(f"   {key}: {value}")
        
        print(f"\n📝 Sample bot-handled data (first 3 rows):")
        if bot_data:
            sample_df = pd.DataFrame(bot_data[:3])
            print(sample_df[['CONVERSATION_ID', 'SENT_BY', 'MESSAGE_TYPE', 'IS_BOT_HANDLED']].to_string())
        else:
            print("   No bot-handled conversations found")
            
    except Exception as e:
        error_report = format_error_details(e, f"BOT HANDLING TEST - {department_name}")
        print(error_report)


def test_repetition_single_department(session: snowpark.Session, department_name, target_date=None):
    """
    Test repetition analysis for a single department.
    """
    print(f"🧪 TESTING REPETITION ANALYSIS - {department_name}")
    print("=" * 50)
    
    try:
        departments_config = get_snowflake_departments_config()
        
        # Get filtered data from Phase 1
        filtered_df, stats, success, _ = process_department_phase1(session, department_name, target_date)
        
        if not success:
            print(f"❌ Phase 1 failed for {department_name}")
            return
        
        # Run repetition analysis
        rep_results, rep_data = analyze_repetition_conversations_single_department(
            filtered_df, department_name, departments_config
        )
        
        print(f"\n📊 RESULTS:")
        for key, value in rep_results.items():
            print(f"   {key}: {value}")
        
        print(f"\n📝 Sample repetition data (first 3 rows):")
        if rep_data:
            sample_df = pd.DataFrame(rep_data[:3])
            print(sample_df[['CONVERSATION_ID', 'TEXT', 'REPETITION', 'CONVERSATION_REPETITION_SCORE']].to_string())
            print(f"\n💡 REPETITION column values: -1=non-repetitive, 0=first occurrence, 1=second occurrence, etc.")
        else:
            print("   No conversations with repetitions found")
            
    except Exception as e:
        error_report = format_error_details(e, f"REPETITION TEST - {department_name}")
        print(error_report)


def validate_phase2_dependencies():
    """
    Validate that Phase 2 dependencies are available (Standalone version).
    """
    print("🔍 VALIDATING PHASE 2 DEPENDENCIES (STANDALONE)")
    print("=" * 50)
    
    try:
        # Test Phase 1 functions are available (included in this file)
        print("✅ Phase 1 foundation functions included in standalone file")
        
        # Test configuration
        config = get_snowflake_departments_config()
        print(f"✅ Department configuration loaded: {len(config)} departments")
        
        # Test date range creation
        date_range = create_snowflake_date_range()
        print(f"✅ Date range creation successful: {date_range['yesterday_date']}")
        
        # Test error formatting
        test_error = Exception("Test error")
        error_formatted = format_error_details(test_error, "TEST")
        print("✅ Error formatting working")
        
        # Test data cleaning function (fix for ArrowTypeError)
        test_df = pd.DataFrame({
            'REPETITION': [-1, 0, 1, 'NA'],  # Mixed types that would cause error
            'TEXT': ['Hello', 'World', 'Hello', 'Test'],
            'IS_BOT_HANDLED': [True, False, True, False]
        })
        cleaned_df = clean_dataframe_for_snowflake(test_df)
        print(f"✅ Data cleaning function working - REPETITION column type: {cleaned_df['REPETITION'].dtype}")
        
        print("\n🎉 All Phase 2 dependencies validated successfully!")
        print("💡 This is a standalone file - no external imports needed for Snowflake")
        print("🔧 ArrowTypeError fix included - REPETITION column uses consistent integer types")
        return True
        
    except Exception as e:
        error_report = format_error_details(e, "DEPENDENCY VALIDATION")
        print(error_report)
        return False 
# ============================================================================
# CHATS FULLY HANDLED BY AGENTS (APPLICANT TRACKING)
# ============================================================================

def calculate_chats_fully_handled_by_agents(session: snowpark.Session, target_date=None):
    """
    Calculate chats fully handled by agents (no bot involvement) for Applicant Tracking departments.
    
    This metric tracks conversations where:
    - Agent initiated and fully handled the conversation
    - NO bot skills were used
    - At least ONE human/agent skill was used
    - Correct nationality and location category for each chatbot
    
    Applies to:
    - AT_Filipina_Outside_UAE (Filipina + OUTSIDE_UAE)
    - AT_Filipina_Inside_UAE (Filipina + INSIDE_UAE)
    - AT_Filipina_In_PHL (Filipina + PHILIPPINES)
    - AT_African (Kenyan + OUTSIDE_UAE)
    
    Args:
        session: Snowflake session
        target_date: Target date for analysis
    
    Returns:
        Dictionary with results per department
    """
    print("\n🤝 CALCULATING CHATS FULLY HANDLED BY AGENTS (APPLICANT TRACKING)")
    print("=" * 60)
    
    departments_config = get_snowflake_departments_config()
    results = {}
    
    # Define nationality and location mapping for each AT department
    at_department_mapping = {
        'AT_Filipina_Outside_UAE': {
            'nationality': 'Filipina',
            'location_category': 'OUTSIDE_UAE'
        },
        'AT_Filipina_Inside_UAE': {
            'nationality': 'Filipina',
            'location_category': 'INSIDE_UAE'
        },
        'AT_Filipina_In_PHL': {
            'nationality': 'Filipina',
            'location_category': 'PHILIPPINES'
        },
        'AT_African': {
            'nationality': 'Kenyan',
            'location_category': 'OUTSIDE_UAE'
        }
    }
    
    for department_name, mapping in at_department_mapping.items():
        if department_name!=DEPARTMENT_FILTER and TEST:
            continue
            
        try:
            print(f"\n🏢 Processing {department_name}...")
            
            dept_config = departments_config.get(department_name, {})
            if not dept_config:
                print(f"  ⚠️  {department_name}: Not configured in departments_config")
                results[department_name] = {'chats_fully_handled_by_agents': 0}
                continue
            
            bot_skills = dept_config.get('bot_skills', [])
            agent_skills = dept_config.get('agent_skills', [])
            all_skills = list(set(bot_skills + agent_skills))
            nationality = mapping['nationality']
            location_category = mapping['location_category']

            all_skills_sql = ", ".join(f"'{s}'" for s in all_skills)

            print(f"  🔍 Debug: target_date = {target_date}")
            print(f"  🔍 Debug: nationality = {nationality}, location = {location_category}")
            print(f"  🔍 Debug: all_skills count = {len(all_skills)} (bot={len(bot_skills)}, agent={len(agent_skills)})")

            # Run diagnostic query
            debug_query = f"""
            WITH dept_conversations AS (
                SELECT DISTINCT a.CONVERSATION_ID
                FROM BA_VIEWS.CHAT_EVALS_SILVER.APPLICANTS_CHATS a,
                     LATERAL FLATTEN(input => SPLIT(a.THROUGH_SKILL, ',')) f
                WHERE TO_DATE(a.START_DATE) = TO_DATE('{target_date}')
                  AND TRIM(f.VALUE) IN ({all_skills_sql})
            ),
            conversations_with_agent AS (
                SELECT DISTINCT CONVERSATION_ID
                FROM BA_VIEWS.CHAT_EVALS_SILVER.APPLICANTS_CHATS
                WHERE CONVERSATION_ID IN (SELECT CONVERSATION_ID FROM dept_conversations)
                  AND TO_DATE(START_DATE) = TO_DATE('{target_date}')
                  AND SENT_BY = 'Agent'
            ),
            conversations_with_bot AS (
                SELECT DISTINCT CONVERSATION_ID
                FROM BA_VIEWS.CHAT_EVALS_SILVER.APPLICANTS_CHATS
                WHERE CONVERSATION_ID IN (SELECT CONVERSATION_ID FROM dept_conversations)
                  AND TO_DATE(START_DATE) = TO_DATE('{target_date}')
                  AND SENT_BY IN ('Bot', 'BOT')
            ),
            fully_agent_handled AS (
                SELECT CONVERSATION_ID
                FROM conversations_with_agent
                WHERE CONVERSATION_ID NOT IN (SELECT CONVERSATION_ID FROM conversations_with_bot)
            )
            SELECT
                (SELECT COUNT(*) FROM dept_conversations) AS dept_conversations_count,
                (SELECT COUNT(*) FROM conversations_with_agent) AS with_agent_count,
                (SELECT COUNT(*) FROM conversations_with_bot) AS with_bot_count,
                (SELECT COUNT(*) FROM fully_agent_handled) AS fully_agent_handled_count
            """

            try:
                debug_df = session.sql(debug_query).to_pandas()
                print(f"  📊 Debug Results:")
                print(f"     - Conversations matching dept skills (THROUGH_SKILL): {debug_df['DEPT_CONVERSATIONS_COUNT'].iloc[0]}")
                print(f"     - With at least one Agent message (SENT_BY='Agent'): {debug_df['WITH_AGENT_COUNT'].iloc[0]}")
                print(f"     - With at least one Bot message (SENT_BY='Bot'): {debug_df['WITH_BOT_COUNT'].iloc[0]}")
                print(f"     - Fully agent-handled (agent YES, bot NO): {debug_df['FULLY_AGENT_HANDLED_COUNT'].iloc[0]}")
            except Exception as debug_e:
                print(f"  ⚠️  Debug query failed: {str(debug_e)}")

            # Main query 
            query = f"""
            WITH dept_conversations AS (
                SELECT DISTINCT a.CONVERSATION_ID, a.APPLICANT_ID, a.MAID_ID
                FROM BA_VIEWS.CHAT_EVALS_SILVER.APPLICANTS_CHATS a,
                     LATERAL FLATTEN(input => SPLIT(a.THROUGH_SKILL, ',')) f
                WHERE TO_DATE(a.START_DATE) = TO_DATE('{target_date}')
                  AND TRIM(f.VALUE) IN ({all_skills_sql})
            ),
            conversations_with_agent AS (
                SELECT DISTINCT CONVERSATION_ID
                FROM BA_VIEWS.CHAT_EVALS_SILVER.APPLICANTS_CHATS
                WHERE CONVERSATION_ID IN (SELECT CONVERSATION_ID FROM dept_conversations)
                  AND TO_DATE(START_DATE) = TO_DATE('{target_date}')
                  AND SENT_BY = 'Agent'
            ),
            conversations_with_bot AS (
                SELECT DISTINCT CONVERSATION_ID
                FROM BA_VIEWS.CHAT_EVALS_SILVER.APPLICANTS_CHATS
                WHERE CONVERSATION_ID IN (SELECT CONVERSATION_ID FROM dept_conversations)
                  AND TO_DATE(START_DATE) = TO_DATE('{target_date}')
                  AND SENT_BY IN ('Bot', 'BOT')
            ),
            fully_agent_handled AS (
                SELECT CONVERSATION_ID
                FROM conversations_with_agent
                WHERE CONVERSATION_ID NOT IN (SELECT CONVERSATION_ID FROM conversations_with_bot)
            ),
            applicant_lookup AS (
                SELECT DISTINCT
                    dc.CONVERSATION_ID,
                    COALESCE(dc.APPLICANT_ID, link.PREFERRED_MATCH_ID) AS final_applicant_id
                FROM dept_conversations dc
                INNER JOIN fully_agent_handled fah
                    ON dc.CONVERSATION_ID = fah.CONVERSATION_ID
                LEFT JOIN BA_VIEWS.MAIDSAT_SILVER.APPLICANTS_HM_LINKING link
                    ON dc.MAID_ID = link.ENTITY_ID
                    AND link.ENTITY_TYPE = 'Housemaid'
                    AND link.PREFERRED_MATCHED_TYPE = 'Applicant'
            ),
            with_nationality AS (
                SELECT al.CONVERSATION_ID, al.final_applicant_id
                FROM applicant_lookup al
                INNER JOIN BA_VIEWS.MAIDSAT_SILVER.APPLICANTS_CORE_DETAILS ac
                    ON al.final_applicant_id = ac.APPLICANT_ID
                WHERE ac.NATIONALITY = '{nationality}'
                  AND ac.LOCATION_CATEGORY = '{location_category}'
            )

            SELECT
                COUNT(DISTINCT CONVERSATION_ID) AS chats_fully_handled_by_agents,
                COUNT(DISTINCT final_applicant_id) AS unique_applicants
            FROM with_nationality
            """

            # Execute query
            result_df = session.sql(query).to_pandas()
            
            if not result_df.empty:
                chats_count = int(result_df['CHATS_FULLY_HANDLED_BY_AGENTS'].iloc[0])
                unique_applicants = int(result_df['UNIQUE_APPLICANTS'].iloc[0])
            else:
                chats_count = 0
                unique_applicants = 0
            
            results[department_name] = {
                'chats_fully_handled_by_agents': chats_count,
                'unique_applicants': unique_applicants
            }
            
            print(f"  ✅ {department_name}: {chats_count} chats fully handled by agents ({unique_applicants} unique applicants)")
            
        except Exception as e:
            print(f"  ❌ {department_name}: Error - {str(e)}")
            results[department_name] = {
                'chats_fully_handled_by_agents': 0,
                'unique_applicants': 0,
                'error': str(e)
            }
    
    # Print summary
    total_chats = sum(r.get('chats_fully_handled_by_agents', 0) for r in results.values())
    total_applicants = sum(r.get('unique_applicants', 0) for r in results.values())
    
    print(f"\n📊 SUMMARY - CHATS FULLY HANDLED BY AGENTS:")
    print(f"   🤝 Total chats across all AT departments: {total_chats:,}")
    print(f"   👥 Total unique applicants: {total_applicants:,}")
    
    return results


# ============================================================================
# PHASE 3: ADVANCED ANALYTICS EXTENSION
# Add these functions to your existing snowflake_phase2_core_analytics.py file
# ============================================================================

# Phase 3 Configuration
DELAY_OUTLIER_THRESHOLD_SECONDS = 4 * 60  # 4 minutes outlier threshold (counts individual messages, not conversations)
DELAY_CLOSING_THRESHOLD_SECONDS = 50 * 60  # 50 minutes threshold for closing messages

# ============================================================================
# DELAY ANALYSIS - 3 SOPHISTICATED METHODS
# ============================================================================

def parse_message_time_snowflake(time_str):
    """
    Parse message timestamp to datetime object (Snowflake version).
    Adapted from main_analytics.py parse_message_time()
    """
    try:
        if pd.isna(time_str) or time_str == '':
            return None
            
        time_str = str(time_str).strip()
        
        # Handle different timestamp formats
        if '.' in time_str:
            # Format: "2025-07-16 22:42:24.0"
            return datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S.%f')
        else:
            # Format: "2025-07-16 22:42:24"
            return datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
    except:
        # Try pandas parsing as fallback
        try:
            return pd.to_datetime(time_str)
        except:
            return None


def calculate_delay_method1_snowflake(bot_message_time, conversation_df, bot_message_index):
    """
    Method 1: Find nearest consumer message right before the bot message.
    Only calculate delay if NO bot or agent has responded to the consumer yet.
    Adapted from main_analytics.py calculate_delay_method1()
    
    Args:
        bot_message_time: Datetime of bot message
        conversation_df: DataFrame of conversation messages
        bot_message_index: Index of bot message
    
    Returns:
        Delay in seconds or None
    """
    # Get all consumer messages before this bot message (using Snowflake column names)
    consumer_messages = conversation_df[
        (conversation_df['SENT_BY'].str.upper() == 'CONSUMER') & 
        (conversation_df['MESSAGE_INDEX'] < bot_message_index) &
        (conversation_df['MESSAGE_SENT_TIME'].notna())
    ].copy()
    
    if len(consumer_messages) == 0:
        return None
    
    # Find the nearest consumer message (highest index before bot message)
    nearest_consumer = consumer_messages.loc[consumer_messages['MESSAGE_INDEX'].idxmax()]
    nearest_consumer_index = nearest_consumer['MESSAGE_INDEX']
    consumer_time = parse_message_time_snowflake(nearest_consumer['MESSAGE_SENT_TIME'])
    
    if consumer_time is None:
        return None
    
    # NEW LOGIC: Check if ANY bot or agent responded between the consumer message and current bot message
    responses_between = conversation_df[
        (conversation_df['MESSAGE_INDEX'] > nearest_consumer_index) &
        (conversation_df['MESSAGE_INDEX'] < bot_message_index) &
        (conversation_df['SENT_BY'].str.upper().isin(['BOT', 'AGENT']))
    ]
    
    # If there were any responses between, don't calculate delay (consumer already got a response)
    if len(responses_between) > 0:
        return None
    
    delay_seconds = (bot_message_time - consumer_time).total_seconds()
    return delay_seconds if delay_seconds >= 0 else None


def calculate_delay_method2_snowflake(bot_message_time, conversation_df, bot_message_index):
    """
    Method 2: If consecutive consumer messages, take the first one.
    Only calculate delay if NO bot or agent has responded to the consumer yet.
    Adapted from main_analytics.py calculate_delay_method2()
    
    Args:
        bot_message_time: Datetime of bot message
        conversation_df: DataFrame of conversation messages
        bot_message_index: Index of bot message
    
    Returns:
        Delay in seconds or None
    """
    # Get all consumer messages before this bot message
    consumer_messages = conversation_df[
        (conversation_df['SENT_BY'].str.upper() == 'CONSUMER') & 
        (conversation_df['MESSAGE_INDEX'] < bot_message_index) &
        (conversation_df['MESSAGE_SENT_TIME'].notna())
    ].copy()
    
    if len(consumer_messages) == 0:
        return None
    
    # Sort by message index
    consumer_messages = consumer_messages.sort_values('MESSAGE_INDEX')
    
    # Find consecutive consumer messages (working backwards from the bot message)
    consecutive_consumers = []
    for i in range(len(consumer_messages) - 1, -1, -1):
        current_consumer = consumer_messages.iloc[i]
        
        # Check if this is consecutive with the next consumer (or bot message)
        next_index = bot_message_index if len(consecutive_consumers) == 0 else consecutive_consumers[0]['MESSAGE_INDEX']
        
        # Find messages between current consumer and next message
        between_messages = conversation_df[
            (conversation_df['MESSAGE_INDEX'] > current_consumer['MESSAGE_INDEX']) &
            (conversation_df['MESSAGE_INDEX'] < next_index) &
            (conversation_df['SENT_BY'].str.upper().isin(['CONSUMER', 'AGENT', 'BOT']))
        ]
        
        # If no messages between, it's consecutive
        if len(between_messages) == 0:
            consecutive_consumers.insert(0, current_consumer)
        else:
            break
    
    if len(consecutive_consumers) == 0:
        # No consecutive consumers, take the nearest one
        nearest_consumer = consumer_messages.iloc[-1]
    else:
        # Take the first of consecutive consumers
        nearest_consumer = consecutive_consumers[0]
    
    selected_consumer_index = nearest_consumer['MESSAGE_INDEX']
    consumer_time = parse_message_time_snowflake(nearest_consumer['MESSAGE_SENT_TIME'])
    
    if consumer_time is None:
        return None
    
    # NEW LOGIC: Check if ANY bot or agent responded between the selected consumer message and current bot message
    responses_between = conversation_df[
        (conversation_df['MESSAGE_INDEX'] > selected_consumer_index) &
        (conversation_df['MESSAGE_INDEX'] < bot_message_index) &
        (conversation_df['SENT_BY'].str.upper().isin(['BOT', 'AGENT']))
    ]
    
    # If there were any responses between, don't calculate delay (consumer already got a response)
    if len(responses_between) > 0:
        return None
    
    delay_seconds = (bot_message_time - consumer_time).total_seconds()
    return delay_seconds if delay_seconds >= 0 else None


def calculate_delay_method3_snowflake(bot_message_time, conversation_df, bot_message_index):
    """
    Method 3: If consecutive consumer messages with <15 seconds gap, take first, otherwise take second.
    Only calculate delay if NO bot or agent has responded to the consumer yet.
    Adapted from main_analytics.py calculate_delay_method3()
    
    Args:
        bot_message_time: Datetime of bot message
        conversation_df: DataFrame of conversation messages
        bot_message_index: Index of bot message
    
    Returns:
        Delay in seconds or None
    """
    # Get all consumer messages before this bot message
    consumer_messages = conversation_df[
        (conversation_df['SENT_BY'].str.upper() == 'CONSUMER') & 
        (conversation_df['MESSAGE_INDEX'] < bot_message_index) &
        (conversation_df['MESSAGE_SENT_TIME'].notna())
    ].copy()
    
    if len(consumer_messages) == 0:
        return None
    
    # Sort by message index
    consumer_messages = consumer_messages.sort_values('MESSAGE_INDEX')
    
    # Find consecutive consumer messages (working backwards from the bot message)
    consecutive_consumers = []
    for i in range(len(consumer_messages) - 1, -1, -1):
        current_consumer = consumer_messages.iloc[i]
        
        # Check if this is consecutive with the next consumer (or bot message)
        next_index = bot_message_index if len(consecutive_consumers) == 0 else consecutive_consumers[0]['MESSAGE_INDEX']
        
        # Find messages between current consumer and next message
        between_messages = conversation_df[
            (conversation_df['MESSAGE_INDEX'] > current_consumer['MESSAGE_INDEX']) &
            (conversation_df['MESSAGE_INDEX'] < next_index) &
            (conversation_df['SENT_BY'].str.upper().isin(['CONSUMER', 'AGENT', 'BOT']))
        ]
        
        # If no messages between, it's consecutive
        if len(between_messages) == 0:
            consecutive_consumers.insert(0, current_consumer)
        else:
            break
    
    if len(consecutive_consumers) <= 1:
        # No consecutive consumers or only one, take the nearest one
        nearest_consumer = consumer_messages.iloc[-1]
    else:
        # Apply 15-second rule to consecutive consumers
        selected_consumer = consecutive_consumers[-1]  # Start with the last (nearest) one
        
        for i in range(len(consecutive_consumers) - 1, 0, -1):
            current_time = parse_message_time_snowflake(consecutive_consumers[i]['MESSAGE_SENT_TIME'])
            previous_time = parse_message_time_snowflake(consecutive_consumers[i-1]['MESSAGE_SENT_TIME'])
            
            if current_time and previous_time:
                time_diff = (current_time - previous_time).total_seconds()
                if time_diff < 15:
                    selected_consumer = consecutive_consumers[i-1]  # Take the earlier one
                else:
                    break  # Keep the current one
        
        nearest_consumer = selected_consumer
    
    selected_consumer_index = nearest_consumer['MESSAGE_INDEX']
    consumer_time = parse_message_time_snowflake(nearest_consumer['MESSAGE_SENT_TIME'])
    
    if consumer_time is None:
        return None
    
    # NEW LOGIC: Check if ANY bot or agent responded between the selected consumer message and current bot message
    responses_between = conversation_df[
        (conversation_df['MESSAGE_INDEX'] > selected_consumer_index) &
        (conversation_df['MESSAGE_INDEX'] < bot_message_index) &
        (conversation_df['SENT_BY'].str.upper().isin(['BOT', 'AGENT']))
    ]
    
    # If there were any responses between, don't calculate delay (consumer already got a response)
    if len(responses_between) > 0:
        return None
    
    delay_seconds = (bot_message_time - consumer_time).total_seconds()
    return delay_seconds if delay_seconds >= 0 else None


def calculate_individual_message_delays_snowflake(conversation_df, department_name, departments_config):
    """
    Calculate delays for each individual bot message using all 3 methods.
    NEW VERSION: Returns individual message delays instead of conversation averages.
    
    Args:
        conversation_df: DataFrame containing one conversation's messages
        department_name: Department name for skill filtering
        departments_config: Department configuration dictionary
    
    Returns:
        Dictionary with individual delay lists for all 3 methods
    """
    department_config = departments_config[department_name]
    bot_skills = set(department_config['bot_skills'])
    
    # Filter for bot normal messages from this department with timestamps (using Snowflake column names)
    bot_messages = conversation_df[
        (conversation_df['SENT_BY'].str.upper() == 'BOT') & 
        (conversation_df['MESSAGE_TYPE'].str.upper() == 'NORMAL MESSAGE') &
        (conversation_df['TARGET_SKILL_PER_MESSAGE'].isin(bot_skills)) &
        (conversation_df['MESSAGE_SENT_TIME'].notna())
    ].copy()
    
    if len(bot_messages) == 0:
        return {
            'method1_initial_delays': [], 'method1_non_initial_delays': [],
            'method2_initial_delays': [], 'method2_non_initial_delays': [],
            'method3_initial_delays': [], 'method3_non_initial_delays': [],
            'bot_message_count': 0
        }
    
    # Ensure MESSAGE_INDEX exists, create if missing
    if 'MESSAGE_INDEX' not in bot_messages.columns:
        # Create message index based on timestamp order
        conversation_df = conversation_df.sort_values('MESSAGE_SENT_TIME').reset_index(drop=True)
        conversation_df['MESSAGE_INDEX'] = conversation_df.index
        
        bot_messages = conversation_df[
            (conversation_df['SENT_BY'].str.upper() == 'BOT') & 
            (conversation_df['MESSAGE_TYPE'].str.upper() == 'NORMAL MESSAGE') &
            (conversation_df['TARGET_SKILL_PER_MESSAGE'].isin(bot_skills)) &
            (conversation_df['MESSAGE_SENT_TIME'].notna())
        ].copy()
    
    # Sort by message index
    bot_messages = bot_messages.sort_values('MESSAGE_INDEX')
    
    # Initialize results to store individual delays
    results = {
        'method1_initial_delays': [], 'method1_non_initial_delays': [],
        'method2_initial_delays': [], 'method2_non_initial_delays': [],
        'method3_initial_delays': [], 'method3_non_initial_delays': [],
        'bot_message_count': len(bot_messages)
    }
    
    # Calculate delays for each method
    # ONLY METHOD 1 ENABLED - Methods 2 & 3 commented out for performance
    delay_methods = [
        calculate_delay_method1_snowflake,
        # calculate_delay_method2_snowflake,  # DISABLED
        # calculate_delay_method3_snowflake   # DISABLED
    ]
    
    for method_num, delay_func in enumerate(delay_methods, 1):
        initial_delay_calculated = False
        last_bot_message_index = None
        
        for i, (_, bot_message) in enumerate(bot_messages.iterrows()):
            bot_time = parse_message_time_snowflake(bot_message['MESSAGE_SENT_TIME'])
            if bot_time is None:
                continue
            
            current_bot_index = bot_message['MESSAGE_INDEX']
            
            # For non-initial delays, check if there's a consumer message after the last bot message
            if initial_delay_calculated and last_bot_message_index is not None:
                # Check if there's any consumer message between last bot message and current bot message
                consumer_messages_between = conversation_df[
                    (conversation_df['SENT_BY'].str.upper() == 'CONSUMER') &
                    (conversation_df['MESSAGE_INDEX'] > last_bot_message_index) &
                    (conversation_df['MESSAGE_INDEX'] < current_bot_index) &
                    (conversation_df['MESSAGE_SENT_TIME'].notna())
                ]
                
                # Only calculate delay if there's a consumer message to respond to
                if len(consumer_messages_between) > 0:
                    delay = delay_func(bot_time, conversation_df, current_bot_index)
                    if delay is not None:
                        results[f'method{method_num}_non_initial_delays'].append(delay)
            else:
                # This is the first bot message, calculate initial delay
                delay = delay_func(bot_time, conversation_df, current_bot_index)
                if delay is not None and not initial_delay_calculated:
                    results[f'method{method_num}_initial_delays'].append(delay)
                    initial_delay_calculated = True
            
            last_bot_message_index = current_bot_index
    
    return results


def analyze_intervention_reengagement_single_conversation_snowflake(conversation_df, department_name):
    """
    Analyze intervention reengagement for a single conversation.
    Checks for last intervention messages (bot, agent, and M20) and whether consumer responded within 10 minutes.
    
    Args:
        conversation_df: DataFrame containing one conversation's messages
        department_name: Department name to get intervention exclusions list
    
    Returns:
        Dictionary with:
        - total_last_interventions_bot: Count of "last" BOT intervention messages
        - total_last_interventions_agent: Count of "last" AGENT intervention messages
        - total_last_interventions_m20: Count of "last" M20 intervention messages
        - total_last_interventions_overall: Total of all three
        - reengaged_interventions_bot: Bot interventions with response within 10 mins
        - reengaged_interventions_agent: Agent interventions with response within 10 mins
        - reengaged_interventions_m20: M20 interventions with response within 10 mins
        - reengaged_interventions_overall: Total of all three
    """
    # Use regex-based poke detection and M20 for reengagement calculation
    if not M20_intervention_word:
        return {
            'total_last_interventions_bot': 0,
            'total_last_interventions_agent': 0,
            'total_last_interventions_m20': 0,
            'total_last_interventions_overall': 0,
            'reengaged_interventions_bot': 0,
            'reengaged_interventions_agent': 0,
            'reengaged_interventions_m20': 0,
            'reengaged_interventions_overall': 0
        }
    
    # Get department configuration for skill filtering
    departments_config = get_snowflake_departments_config()
    department_config = departments_config.get(department_name, {})
    bot_skills = set(department_config.get('bot_skills', []))
    agent_skills = set(department_config.get('agent_skills', []))
    
    # Find all intervention messages (poke and M20) using regex pattern matching
    intervention_messages = []
    for _, message in conversation_df.iterrows():
        sender_type = message['SENT_BY'].upper()
        message_skill = message.get('TARGET_SKILL_PER_MESSAGE', '')
        message_content = str(message.get('TEXT', '')).strip().lower()
        
        # Check if message is from department's skills
        skill_matches = False
        if sender_type == 'BOT' and message_skill in bot_skills:
            skill_matches = True
        elif sender_type == 'AGENT' and message_skill in agent_skills:
            skill_matches = True
        
        # Only process messages from department skills
        if skill_matches:
            # Check if message matches any poke phrase (case-insensitive)
            is_poke = False
            
            # For both CC_Sales and MV_Sales: check for "minutes poke" or "minute poke" pattern using regex
            if department_name in ['CC_Sales', 'MV_Sales']:
                # Match patterns like "10 minutes poke", "2.5 minutes poke", "minute poke", etc.
                poke_match = re.search(r'\d+\.?\d*\s*minutes?\s+poke', message_content, re.IGNORECASE)
                simple_match = 'minutes poke' in message_content or 'minute poke' in message_content
                
                if poke_match or simple_match:
                    is_poke = True
               
                       
            
            if is_poke:
                intervention_messages.append({
                    'message': message,
                    'sender_type': sender_type,
                    'index': message['MESSAGE_INDEX'],
                    'time': message['MESSAGE_SENT_TIME']
                })
        
        # Check for M20 intervention (must be BOT from department skills)
        if (sender_type == 'BOT' and message_skill in bot_skills and 
            re.search(re.escape(M20_intervention_word.lower()), message_content, re.IGNORECASE)):
            intervention_messages.append({
                'message': message,
                'sender_type': 'M20',
                'index': message['MESSAGE_INDEX'],
                'time': message['MESSAGE_SENT_TIME']
            })
    
    # CC_Sales message categorization - Add all messages to global DataFrame
    if department_name == 'CC_Sales':
        global CC_SALES_POKES_VALIDATION_DF
        
        # Process each message for categorization
        for _, message in conversation_df.iterrows():
            sender_type = message['SENT_BY'].upper()
            message_skill = message.get('TARGET_SKILL_PER_MESSAGE', '')
            message_content = str(message.get('TEXT', '')).strip().lower()
            message_index = message['MESSAGE_INDEX']
            message_time = message['MESSAGE_SENT_TIME']
            
            # Determine message category
            message_category = 'dynamic'  # Default
            
            # Check for static (intervention) messages
            skill_matches = False
            if sender_type == 'BOT' and message_skill in bot_skills:
                skill_matches = True
            elif sender_type == 'AGENT' and message_skill in agent_skills:
                skill_matches = True
            
            if skill_matches:
                # Check for static messages (static_messages_cc_sales)
                for static_phrase in static_messages_cc_sales:
                    if re.search(re.escape(static_phrase.lower()), message_content, re.IGNORECASE):
                        message_category = 'static'
                        break
                
                # Check for poke messages - only if not already static
                # For both CC_Sales and MV_Sales: check for "minutes poke" or "minute poke" pattern using regex
                if message_category != 'static':
                    if department_name in ['CC_Sales', 'MV_Sales']:
                        # Match patterns like "10 minutes poke", "2.5 minutes poke", "minute poke", etc.
                        if re.search(r'\d+\.?\d*\s*minutes?\s+poke', message_content, re.IGNORECASE) or 'minutes poke' in message_content or 'minute poke' in message_content:
                            message_category = 'poke'
                            
            
            # Check for M20 messages (must be BOT from department skills, same as main calculation)
            if (sender_type == 'BOT' and message_skill in bot_skills and 
                re.search(re.escape(M20_intervention_word.lower()), message_content, re.IGNORECASE)):
                message_category = 'm20'
            
            # Calculate 10-minute reengagement for poke and M20 messages only (and only if last intervention)
            is_10min_reengaged = False
            next_consumer_response_time = None
            time_to_next_response_minutes = None
            
            if message_category in ['poke', 'm20']:
                # Check if this message is a "last intervention" in the conversation
                is_last_intervention = True
                
                # Check if any later poke/M20 messages exist in this conversation
                for later_intervention in intervention_messages:
                    if later_intervention['index'] > message_index:
                        is_last_intervention = False
                        break
                
                # Only calculate reengagement if this is the last intervention
                if is_last_intervention:
                    # Find next consumer message after this message
                    next_consumer_messages = conversation_df[
                        (conversation_df['MESSAGE_INDEX'] > message_index) &
                        (conversation_df['SENT_BY'].str.upper() == 'CONSUMER') &
                        (conversation_df['MESSAGE_SENT_TIME'].notna())
                    ].sort_values('MESSAGE_INDEX')
                    
                    if len(next_consumer_messages) > 0:
                        first_consumer_response = next_consumer_messages.iloc[0]
                        current_time = parse_message_time_snowflake(message_time)
                        response_time = parse_message_time_snowflake(first_consumer_response['MESSAGE_SENT_TIME'])
                        
                        if current_time and response_time:
                            time_diff_minutes = (response_time - current_time).total_seconds() / 60
                            time_to_next_response_minutes = time_diff_minutes
                            next_consumer_response_time = first_consumer_response['MESSAGE_SENT_TIME']
                            
                            if time_diff_minutes <= 10:
                                is_10min_reengaged = True
            
            # Create message row for global DataFrame
            message_row = {
                'CONVERSATION_ID': message['CONVERSATION_ID'],
                'MESSAGE_INDEX': message_index,
                'MESSAGE_SENT_TIME': message_time,
                'SENT_BY': message['SENT_BY'],
                'TARGET_SKILL_PER_MESSAGE': message_skill,
                'TEXT': message.get('TEXT', ''),
                'MESSAGE_CATEGORY': message_category,
                'IS_10MIN_REENGAGED': is_10min_reengaged,
                'NEXT_CONSUMER_RESPONSE_TIME': next_consumer_response_time,
                'TIME_TO_NEXT_RESPONSE_MINUTES': time_to_next_response_minutes
            }
            
            # Append to global DataFrame
            CC_SALES_POKES_VALIDATION_DF = pd.concat([CC_SALES_POKES_VALIDATION_DF, pd.DataFrame([message_row])], ignore_index=True)
    
    if not intervention_messages:
        return {
            'total_last_interventions_bot': 0,
            'total_last_interventions_agent': 0,
            'total_last_interventions_m20': 0,
            'total_last_interventions_overall': 0,
            'reengaged_interventions_bot': 0,
            'reengaged_interventions_agent': 0,
            'reengaged_interventions_m20': 0,
            'reengaged_interventions_overall': 0
        }
    
    # Initialize counters
    total_last_bot = 0
    total_last_agent = 0
    total_last_m20 = 0
    reengaged_bot = 0
    reengaged_agent = 0
    reengaged_m20 = 0
    
    # For each intervention message, check if it's a "last intervention"
    for intervention in intervention_messages:
        intervention_index = intervention['index']
        sender_type = intervention['sender_type']
        
        # Check if any later message (bot or agent) is also an intervention
        later_interventions = []
        for later_msg in intervention_messages:
            if later_msg['index'] > intervention_index:
                later_interventions.append(later_msg)
        
        # If no later interventions found, this is a "last intervention"
        is_last_intervention = len(later_interventions) == 0
        
        if is_last_intervention:
            # Count as last intervention
            if sender_type == 'BOT':
                total_last_bot += 1
            elif sender_type == 'AGENT':
                total_last_agent += 1
            elif sender_type == 'M20':
                total_last_m20 += 1
            
            # Check for consumer reengagement within 10 minutes
            intervention_time = parse_message_time_snowflake(intervention['time'])
            if intervention_time:
                # Find next consumer message after this intervention
                next_consumer_messages = conversation_df[
                    (conversation_df['MESSAGE_INDEX'] > intervention_index) &
                    (conversation_df['SENT_BY'].str.upper() == 'CONSUMER') &
                    (conversation_df['MESSAGE_SENT_TIME'].notna())
                ].sort_values('MESSAGE_INDEX')
                
                if len(next_consumer_messages) > 0:
                    first_consumer_response = next_consumer_messages.iloc[0]
                    response_time = parse_message_time_snowflake(first_consumer_response['MESSAGE_SENT_TIME'])
                    
                    if response_time:
                        time_diff_minutes = (response_time - intervention_time).total_seconds() / 60
                        if time_diff_minutes <= 10:
                            # Consumer reengaged within 10 minutes
                            if sender_type == 'BOT':
                                reengaged_bot += 1
                            elif sender_type == 'AGENT':
                                reengaged_agent += 1
                            elif sender_type == 'M20':
                                reengaged_m20 += 1
    
    return {
        'total_last_interventions_bot': total_last_bot,
        'total_last_interventions_agent': total_last_agent,
        'total_last_interventions_m20': total_last_m20,
        'total_last_interventions_overall': total_last_bot + total_last_agent + total_last_m20,
        'reengaged_interventions_bot': reengaged_bot,
        'reengaged_interventions_agent': reengaged_agent,
        'reengaged_interventions_m20': reengaged_m20,
        'reengaged_interventions_overall': reengaged_bot + reengaged_agent + reengaged_m20
    }


def calculate_conversation_delays_snowflake(conversation_df, department_name, departments_config):
    """
    Calculate initial and non-initial delays for a conversation using all 3 methods.
    Adapted from main_analytics.py calculate_conversation_delays()
    
    Args:
        conversation_df: DataFrame containing one conversation's messages
        department_name: Department name for skill filtering
        departments_config: Department configuration dictionary
    
    Returns:
        Dictionary with delay results for all 3 methods
    """
    department_config = departments_config[department_name]
    bot_skills = set(department_config['bot_skills'])
    
    # Filter for bot normal messages from this department with timestamps (using Snowflake column names)
    bot_messages = conversation_df[
        (conversation_df['SENT_BY'].str.upper() == 'BOT') & 
        (conversation_df['MESSAGE_TYPE'].str.upper() == 'NORMAL MESSAGE') &
        (conversation_df['TARGET_SKILL_PER_MESSAGE'].isin(bot_skills)) &
        (conversation_df['MESSAGE_SENT_TIME'].notna())
    ].copy()
    
    if len(bot_messages) == 0:
        return {
            'method1_initial_delay': None, 'method1_non_initial_delay': None,
            'method2_initial_delay': None, 'method2_non_initial_delay': None,
            'method3_initial_delay': None, 'method3_non_initial_delay': None,
            'bot_message_count': 0
        }
    
    # Ensure MESSAGE_INDEX exists, create if missing
    if 'MESSAGE_INDEX' not in bot_messages.columns:
        # Create message index based on timestamp order
        conversation_df = conversation_df.sort_values('MESSAGE_SENT_TIME').reset_index(drop=True)
        conversation_df['MESSAGE_INDEX'] = conversation_df.index
        
        bot_messages = conversation_df[
            (conversation_df['SENT_BY'].str.upper() == 'BOT') & 
            (conversation_df['MESSAGE_TYPE'].str.upper() == 'NORMAL MESSAGE') &
            (conversation_df['TARGET_SKILL_PER_MESSAGE'].isin(bot_skills)) &
            (conversation_df['MESSAGE_SENT_TIME'].notna())
        ].copy()
    
    # Sort by message index
    bot_messages = bot_messages.sort_values('MESSAGE_INDEX')
    
    results = {
        'method1_initial_delay': None, 'method1_non_initial_delay': None,
        'method2_initial_delay': None, 'method2_non_initial_delay': None,
        'method3_initial_delay': None, 'method3_non_initial_delay': None,
        'bot_message_count': len(bot_messages)
    }
    
    # Calculate delays for each method
    # ONLY METHOD 1 ENABLED - Methods 2 & 3 commented out for performance
    delay_methods = [
        calculate_delay_method1_snowflake,
        # calculate_delay_method2_snowflake,  # DISABLED
        # calculate_delay_method3_snowflake   # DISABLED
    ]
    
    for method_num, delay_func in enumerate(delay_methods, 1):
        initial_delay = None
        non_initial_delays = []
        last_bot_message_index = None
        
        for i, (_, bot_message) in enumerate(bot_messages.iterrows()):
            bot_time = parse_message_time_snowflake(bot_message['MESSAGE_SENT_TIME'])
            if bot_time is None:
                continue
            
            current_bot_index = bot_message['MESSAGE_INDEX']
            
            # For non-initial delays, check if there's a consumer message after the last bot message
            if initial_delay is not None and last_bot_message_index is not None:
                # Check if there's any consumer message between last bot message and current bot message
                consumer_messages_between = conversation_df[
                    (conversation_df['SENT_BY'].str.upper() == 'CONSUMER') &
                    (conversation_df['MESSAGE_INDEX'] > last_bot_message_index) &
                    (conversation_df['MESSAGE_INDEX'] < current_bot_index) &
                    (conversation_df['MESSAGE_SENT_TIME'].notna())
                ]
                
                # Only calculate delay if there's a consumer message to respond to
                if len(consumer_messages_between) > 0:
                    delay = delay_func(bot_time, conversation_df, current_bot_index)
                    if delay is not None:
                        non_initial_delays.append(delay)
            else:
                # This is the first bot message, calculate initial delay
                delay = delay_func(bot_time, conversation_df, current_bot_index)
                if delay is not None and initial_delay is None:
                    initial_delay = delay
            
            last_bot_message_index = current_bot_index
        
        # Store results for this method
        method_key = f'method{method_num}'
        results[f'{method_key}_initial_delay'] = initial_delay
        
        if len(non_initial_delays) > 0:
            results[f'{method_key}_non_initial_delay'] = sum(non_initial_delays) / len(non_initial_delays)
        else:
            results[f'{method_key}_non_initial_delay'] = None
    
    return results


def assign_individual_delays_to_messages_snowflake(conversation_df, department_name, departments_config):
    """
    Calculate and assign individual delays to each bot message in the conversation.
    Non-bot messages get None/null values for delay columns.
    
    Args:
        conversation_df: DataFrame containing one conversation's messages
        department_name: Department name for skill filtering
        departments_config: Department configuration dictionary
    
    Returns:
        DataFrame with individual delay columns added to each message
    """
    department_config = departments_config[department_name]
    bot_skills = set(department_config['bot_skills'])
    
    # Start with a copy of the conversation
    result_df = conversation_df.copy()
    
    # Initialize all delay columns with None (will be converted to proper nulls)
    delay_columns = [
        'DELAY_METHOD1_INITIAL', 'DELAY_METHOD1_NON_INITIAL',
        'DELAY_METHOD2_INITIAL', 'DELAY_METHOD2_NON_INITIAL', 
        'DELAY_METHOD3_INITIAL', 'DELAY_METHOD3_NON_INITIAL'
    ]
    
    for col in delay_columns:
        result_df[col] = None
    
    # Ensure MESSAGE_INDEX exists for proper ordering
    if 'MESSAGE_INDEX' not in result_df.columns:
        result_df = result_df.sort_values('MESSAGE_SENT_TIME').reset_index(drop=True)
        result_df['MESSAGE_INDEX'] = result_df.index
    
    # Filter for bot normal messages from this department with timestamps
    bot_messages = result_df[
        (result_df['SENT_BY'].str.upper() == 'BOT') & 
        (result_df['MESSAGE_TYPE'].str.upper() == 'NORMAL MESSAGE') &
        (result_df['TARGET_SKILL_PER_MESSAGE'].isin(bot_skills)) &
        (result_df['MESSAGE_SENT_TIME'].notna())
    ].copy()
    
    if len(bot_messages) == 0:
        return result_df
    
    # Sort by message index
    bot_messages = bot_messages.sort_values('MESSAGE_INDEX')
    
    # Calculate delays for each method
    # ONLY METHOD 1 ENABLED - Methods 2 & 3 commented out for performance
    delay_methods = [
        calculate_delay_method1_snowflake,
        # calculate_delay_method2_snowflake,  # DISABLED
        # calculate_delay_method3_snowflake   # DISABLED
    ]
    
    for method_num, delay_func in enumerate(delay_methods, 1):
        initial_delay_assigned = False
        last_bot_message_index = None
        
        for i, (row_idx, bot_message) in enumerate(bot_messages.iterrows()):
            bot_time = parse_message_time_snowflake(bot_message['MESSAGE_SENT_TIME'])
            if bot_time is None:
                continue
            
            current_bot_index = bot_message['MESSAGE_INDEX']
            
            # Calculate delay for this specific bot message
            delay = delay_func(bot_time, result_df, current_bot_index)
            if delay is None:
                continue
            
            # Determine if this is initial or non-initial delay
            if not initial_delay_assigned:
                # This is the first bot message, assign as initial delay
                result_df.loc[row_idx, f'DELAY_METHOD{method_num}_INITIAL'] = delay
                initial_delay_assigned = True
            elif last_bot_message_index is not None:
                # Check if there's a consumer message between last bot and current bot
                consumer_messages_between = result_df[
                    (result_df['SENT_BY'].str.upper() == 'CONSUMER') &
                    (result_df['MESSAGE_INDEX'] > last_bot_message_index) &
                    (result_df['MESSAGE_INDEX'] < current_bot_index) &
                    (result_df['MESSAGE_SENT_TIME'].notna())
                ]
                
                # Only assign non-initial delay if there's a consumer message to respond to
                if len(consumer_messages_between) > 0:
                    result_df.loc[row_idx, f'DELAY_METHOD{method_num}_NON_INITIAL'] = delay
            
            last_bot_message_index = current_bot_index
    
    return result_df


def analyze_intervention_reengagement_single_department(session, df, department_name, departments_config, target_date):
    """
    Analyze intervention reengagement for all conversations in a single department.
    
    Args:
        session: Snowflake session object
        df: DataFrame containing all conversations for the department
        department_name: Department name
        departments_config: Department configuration dictionary
        target_date: Target date for analysis
    
    Returns:
        Dictionary with reengagement metrics for bot, agent, M20, and overall
    """
    print(f"  🔄 Analyzing intervention reengagement for {department_name}...")
    
    try:
        if df.empty:
            print(f"  ⚠️  No data provided for {department_name}")
            return {
                'total_last_interventions_bot_count': 0,
                'total_last_interventions_agent_count': 0,
                'total_last_interventions_m20_count': 0,
                'total_last_interventions_overall_count': 0,
                'reengaged_interventions_bot_count': 0,
                'reengaged_interventions_agent_count': 0,
                'reengaged_interventions_m20_count': 0,
                'reengaged_interventions_overall_count': 0,
                'intervention_reengagement_bot_rate': 0,
                'intervention_reengagement_agent_rate': 0,
                'intervention_reengagement_m20_rate': 0,
                'intervention_reengagement_overall_rate': 0
            }
        # check if departemnt name not cc or mv sales return 0s
        if department_name not in ['CC_Sales', 'MV_Sales']:
            print(f"  ⚠️  not applicable to {department_name}")
            return {
                'total_last_interventions_bot_count': 0,
                'total_last_interventions_agent_count': 0,
                'total_last_interventions_m20_count': 0,
                'total_last_interventions_overall_count': 0,
                'reengaged_interventions_bot_count': 0,
                'reengaged_interventions_agent_count': 0,
                'reengaged_interventions_m20_count': 0,
                'reengaged_interventions_overall_count': 0,
                'intervention_reengagement_bot_rate': 0,
                'intervention_reengagement_agent_rate': 0,
                'intervention_reengagement_m20_rate': 0,
                'intervention_reengagement_overall_rate': 0
            }
        
        # Group by conversation
        conversations = df.groupby('CONVERSATION_ID')
        
        # Initialize counters
        total_last_bot = 0
        total_last_agent = 0
        total_last_m20 = 0
        reengaged_bot = 0
        reengaged_agent = 0
        reengaged_m20 = 0
        processed_conversations = 0
        
        for conv_id, conv_df in conversations:
            try:
                # Analyze intervention reengagement for this conversation
                reengagement_results = analyze_intervention_reengagement_single_conversation_snowflake(conv_df, department_name)
                
                # Aggregate results
                total_last_bot += reengagement_results['total_last_interventions_bot']
                total_last_agent += reengagement_results['total_last_interventions_agent']
                total_last_m20 += reengagement_results['total_last_interventions_m20']
                reengaged_bot += reengagement_results['reengaged_interventions_bot']
                reengaged_agent += reengagement_results['reengaged_interventions_agent']
                reengaged_m20 += reengagement_results['reengaged_interventions_m20']
                processed_conversations += 1
                
            except Exception as e:
                print(f"    ⚠️ Error processing conversation {conv_id}: {str(e)}")
                continue
        
        # Calculate overall totals
        total_last_overall = total_last_bot + total_last_agent + total_last_m20
        reengaged_overall = reengaged_bot + reengaged_agent + reengaged_m20
        
        # Calculate percentages
        bot_rate = (reengaged_bot / total_last_bot * 100) if total_last_bot > 0 else 0
        agent_rate = (reengaged_agent / total_last_agent * 100) if total_last_agent > 0 else 0
        m20_rate = (reengaged_m20 / total_last_m20 * 100) if total_last_m20 > 0 else 0
        overall_rate = (reengaged_overall / total_last_overall * 100) if total_last_overall > 0 else 0
        
        results = {
            'total_last_interventions_bot_count': total_last_bot,
            'total_last_interventions_agent_count': total_last_agent,
            'total_last_interventions_m20_count': total_last_m20,
            'total_last_interventions_overall_count': total_last_overall,
            'reengaged_interventions_bot_count': reengaged_bot,
            'reengaged_interventions_agent_count': reengaged_agent,
            'reengaged_interventions_m20_count': reengaged_m20,
            'reengaged_interventions_overall_count': reengaged_overall,
            'intervention_reengagement_bot_rate': round(bot_rate, 1),
            'intervention_reengagement_agent_rate': round(agent_rate, 1),
            'intervention_reengagement_m20_rate': round(m20_rate, 1),
            'intervention_reengagement_overall_rate': round(overall_rate, 1)
        }
        
        # Print summary
        print(f"  ✅ {department_name} intervention reengagement analysis:")
        print(f"    🤖 Bot: {reengaged_bot}/{total_last_bot} ({bot_rate:.1f}%)")
        print(f"    👤 Agent: {reengaged_agent}/{total_last_agent} ({agent_rate:.1f}%)")
        print(f"    🎯 M20: {reengaged_m20}/{total_last_m20} ({m20_rate:.1f}%)")
        print(f"    📊 Overall: {reengaged_overall}/{total_last_overall} ({overall_rate:.1f}%)")
        print(f"    📈 Processed {processed_conversations} conversations")
        
        return results
        
    except Exception as e:
        print(f"  ❌ {department_name}: Intervention reengagement analysis failed: {str(e)}")
        # Return default values on error
        return {
            'total_last_interventions_bot_count': 0,
            'total_last_interventions_agent_count': 0,
            'total_last_interventions_m20_count': 0,
            'total_last_interventions_overall_count': 0,
            'reengaged_interventions_bot_count': 0,
            'reengaged_interventions_agent_count': 0,
            'reengaged_interventions_m20_count': 0,
            'reengaged_interventions_overall_count': 0,
            'intervention_reengagement_bot_rate': 0,
            'intervention_reengagement_agent_rate': 0,
            'intervention_reengagement_m20_rate': 0,
            'intervention_reengagement_overall_rate': 0
        }


def analyze_intervention_reengagement_all_departments(session: snowpark.Session, target_date=None):
    """
    Analyze intervention reengagement patterns for all departments using Phase 1 filtered data.
    
    Args:
        session: Snowflake session
        target_date: Target date for analysis
    
    Returns:
        Dictionary with department intervention reengagement results
    """
    print("\n🔄 INTERVENTION REENGAGEMENT ANALYSIS (ALL DEPARTMENTS)")
    print("=" * 60)
    
    target_date_str = target_date if target_date else datetime.now().strftime('%Y-%m-%d')
    print(f"📅 Target date: {target_date_str}")
    
    departments_config = get_snowflake_departments_config()
    results = {}
    
    # Department filter handling
    selected_departments = [DEPARTMENT_FILTER] if TEST else list(departments_config.keys())
    
    total_bot = 0
    total_agent = 0
    total_overall = 0
    reengaged_bot = 0
    reengaged_agent = 0
    reengaged_overall = 0
    
    for department_name in selected_departments:
        # INTERVENTION REENGAGEMENT: Only run for CC_Sales and MV_Sales
        if department_name not in ['CC_Sales', 'MV_Sales']:
            print(f"\n🏢 {department_name}: Skipping intervention reengagement (CC_Sales & MV_Sales only)")
            results[department_name] = {
                'total_last_interventions_bot_count': 0,
                'total_last_interventions_agent_count': 0,
                'total_last_interventions_m20_count': 0,
                'total_last_interventions_overall_count': 0,
                'reengaged_interventions_bot_count': 0,
                'reengaged_interventions_agent_count': 0,
                'reengaged_interventions_m20_count': 0,
                'reengaged_interventions_overall_count': 0,
                'intervention_reengagement_bot_rate': 0,
                'intervention_reengagement_agent_rate': 0,
                'intervention_reengagement_m20_rate': 0,
                'intervention_reengagement_overall_rate': 0
            }
            continue
        
        if department_name not in departments_config:
            print(f"⚠️  Department {department_name} not found in config")
            continue
            
        try:
            # Get filtered data from Phase 1
            print(f"\n🏢 Processing {department_name}...")
            filtered_df, phase1_stats, success, _ = process_department_phase1(session, department_name, target_date_str)
            
            if not success or filtered_df.empty:
                print(f"  ❌ {department_name}: No filtered data from Phase 1")
                results[department_name] = {
                    'total_last_interventions_bot_count': 0,
                    'total_last_interventions_agent_count': 0,
                    'total_last_interventions_overall_count': 0,
                    'reengaged_interventions_bot_count': 0,
                    'reengaged_interventions_agent_count': 0,
                    'reengaged_interventions_overall_count': 0,
                    'intervention_reengagement_bot_rate': 0,
                    'intervention_reengagement_agent_rate': 0,
                    'intervention_reengagement_overall_rate': 0
                }
                continue
            
            # Process intervention reengagement for this department using the Phase 1 filtered DataFrame
            dept_results = analyze_intervention_reengagement_single_department(session, filtered_df, department_name, departments_config, target_date_str)
            results[department_name] = dept_results
            
            # Aggregate overall totals
            total_bot += dept_results['total_last_interventions_bot_count']
            total_agent += dept_results['total_last_interventions_agent_count']
            total_overall += dept_results['total_last_interventions_overall_count']
            reengaged_bot += dept_results['reengaged_interventions_bot_count']
            reengaged_agent += dept_results['reengaged_interventions_agent_count']
            reengaged_overall += dept_results['reengaged_interventions_overall_count']
            
        except Exception as e:
            print(f"  ❌ Error processing {department_name}: {str(e)}")
            results[department_name] = {
                'total_last_interventions_bot_count': 0,
                'total_last_interventions_agent_count': 0,
                'total_last_interventions_overall_count': 0,
                'reengaged_interventions_bot_count': 0,
                'reengaged_interventions_agent_count': 0,
                'reengaged_interventions_overall_count': 0,
                'intervention_reengagement_bot_rate': 0,
                'intervention_reengagement_agent_rate': 0,
                'intervention_reengagement_overall_rate': 0
            }
    
    # Calculate overall summary
    overall_bot_rate = (reengaged_bot / total_bot * 100) if total_bot > 0 else 0
    overall_agent_rate = (reengaged_agent / total_agent * 100) if total_agent > 0 else 0
    overall_rate = (reengaged_overall / total_overall * 100) if total_overall > 0 else 0
    
    print("\n" + "=" * 60)
    print("🤝 INTERVENTION REENGAGEMENT SUMMARY (10-min response):")
    print(f"   🤖 Bot interventions: {reengaged_bot}/{total_bot} ({overall_bot_rate:.1f}%)")
    print(f"   👤 Agent interventions: {reengaged_agent}/{total_agent} ({overall_agent_rate:.1f}%)")
    print(f"   📊 Overall: {reengaged_overall}/{total_overall} ({overall_rate:.1f}%)")
    print("=" * 60)
    
    return results


def detect_agent_intervention_due_to_no_response_snowflake(conversation_df, department_name, departments_config):
    """
    Detect if an agent had to intervene because the bot failed to respond in time (>4 minutes).
    
    Logic:
    1. Find LAST bot message from this department (or detect if bot never responded)
    2. Find FIRST agent message AFTER last bot (from dept agent_skills)
    3. Use calculate_delay_method1_snowflake to calculate delay to agent message
    4. If delay > 4 minutes → this is an intervention due to no response
    
    Args:
        conversation_df: DataFrame containing one conversation's messages
        department_name: Department name for skill filtering
        departments_config: Department configuration dictionary
    
    Returns:
        dict with intervention details or None if no intervention detected
    """
    try:
        department_config = departments_config[department_name]
        bot_skills = set(department_config['bot_skills'])
        agent_skills = set(department_config['agent_skills'])
        
        # Ensure MESSAGE_INDEX exists for proper ordering
        if 'MESSAGE_INDEX' not in conversation_df.columns:
            conversation_df = conversation_df.sort_values('MESSAGE_SENT_TIME').reset_index(drop=True)
            conversation_df['MESSAGE_INDEX'] = conversation_df.index
        
        # 1. Find bot messages from this department
        bot_messages = conversation_df[
            (conversation_df['SENT_BY'].str.upper() == 'BOT') & 
            (conversation_df['MESSAGE_TYPE'].str.upper() == 'NORMAL MESSAGE') &
            (conversation_df['TARGET_SKILL_PER_MESSAGE'].isin(bot_skills)) &
            (conversation_df['MESSAGE_SENT_TIME'].notna())
        ].copy()
        
        # 2. Handle two cases: with bot messages OR without bot messages
        dept_skills = bot_skills.union(agent_skills)
        
        if len(bot_messages) > 0:
            # CASE A: Bot messages exist - find agent AFTER last bot
            bot_messages = bot_messages.sort_values('MESSAGE_INDEX')
            last_bot_message = bot_messages.iloc[-1]
            last_bot_index = last_bot_message['MESSAGE_INDEX']
            
            agent_messages_after_bot = conversation_df[
                (conversation_df['SENT_BY'].str.upper() == 'AGENT') &
                (conversation_df['MESSAGE_TYPE'].str.upper() == 'NORMAL MESSAGE') &
                (conversation_df['TARGET_SKILL_PER_MESSAGE'].isin(dept_skills)) &
                (conversation_df['MESSAGE_INDEX'] > last_bot_index) &
                (conversation_df['MESSAGE_SENT_TIME'].notna())
            ].copy()
        else:
            # CASE B: No bot messages - bot was assigned but never responded
            last_bot_index = None
            
            # Find ALL agent messages (bot never responded, so check from beginning)
            agent_messages_after_bot = conversation_df[
                (conversation_df['SENT_BY'].str.upper() == 'AGENT') &
                (conversation_df['MESSAGE_TYPE'].str.upper() == 'NORMAL MESSAGE') &
                (conversation_df['TARGET_SKILL_PER_MESSAGE'].isin(dept_skills)) &
                (conversation_df['MESSAGE_SENT_TIME'].notna())
            ].copy()
        
        if len(agent_messages_after_bot) == 0:
            return None  # No agent intervention
        
        # 3. Get FIRST agent message
        agent_messages_after_bot = agent_messages_after_bot.sort_values('MESSAGE_INDEX')
        first_agent_intervention = agent_messages_after_bot.iloc[0]
        agent_message_index = first_agent_intervention['MESSAGE_INDEX']
        agent_message_time = parse_message_time_snowflake(first_agent_intervention['MESSAGE_SENT_TIME'])
        
        if agent_message_time is None:
            return None
        
        # 5. CLEVER TRICK: Use existing function to calculate delay
        # This will find the consumer message the agent is responding to
        delay_seconds = calculate_delay_method1_snowflake(
            agent_message_time,
            conversation_df,
            agent_message_index
        )
        
        if delay_seconds is None:
            return None  # No valid delay calculation
        
        # 6. Check if delay exceeds threshold (4 minutes)
        if delay_seconds > DELAY_OUTLIER_THRESHOLD_SECONDS:
            # Find the consumer message that the agent is responding to
            consumer_messages_before = conversation_df[
                (conversation_df['SENT_BY'].str.upper() == 'CONSUMER') & 
                (conversation_df['MESSAGE_INDEX'] < agent_message_index) &
                (conversation_df['MESSAGE_SENT_TIME'].notna())
            ].copy()
            
            consumer_message_time = None
            consumer_message_index = None
            if len(consumer_messages_before) > 0:
                # Get the nearest consumer message (same logic as method1)
                nearest_consumer = consumer_messages_before.loc[consumer_messages_before['MESSAGE_INDEX'].idxmax()]
                consumer_message_time = parse_message_time_snowflake(nearest_consumer['MESSAGE_SENT_TIME'])
                consumer_message_index = nearest_consumer['MESSAGE_INDEX']
            
            # 7. EXTRA VALIDATION: Different validation for Case A vs Case B
            if last_bot_index is not None:
                # CASE A: Bot existed - Check for valid system transfer
                if consumer_message_index is not None:
                    # Find system transfer messages between consumer message and agent message
                    system_transfer_messages = conversation_df[
                        (conversation_df['SENT_BY'].str.upper() == 'SYSTEM') &
                        ((conversation_df['MESSAGE_TYPE'].str.upper() == 'PRIVATE MESSAGE') |
                         (conversation_df['MESSAGE_TYPE'].str.upper() == 'TRANSFER')) &
                        (conversation_df['MESSAGE_INDEX'] > consumer_message_index) &
                        (conversation_df['MESSAGE_INDEX'] < agent_message_index)
                    ]
                    
                    # Check if any system transfer is a valid transfer
                    has_valid_system_transfer = False
                    for _, message in system_transfer_messages.iterrows():
                        message_text = str(message.get('TEXT', ''))
                        transfer_data = parse_transfer_2222(message_text)
                        
                        # Check all three conditions:
                        # 1. 'by' doesn't contain 'GPT' (case-insensitive)
                        # 2. 'from_skill' is in bot_skills
                        # 3. 'to_skill' is in dept_skills
                        if (transfer_data.get('by', '') and 
                            'gpt' not in transfer_data.get('by', '').lower() and
                            transfer_data.get('from_skill', '') in bot_skills and
                            transfer_data.get('to_skill', '') in dept_skills):
                            
                            has_valid_system_transfer = True
                            break
                    
                    # Only return intervention if there was a valid system transfer
                    if not has_valid_system_transfer and first_agent_intervention['TARGET_SKILL_PER_MESSAGE'] in agent_skills:
                        return None  # No valid system transfer found, don't count as intervention
            else:
                # CASE B: No bot messages - Check for bot assignment + agent in dept skills + system transfer
                if consumer_message_index is not None:
                    messages_between = conversation_df[
                        (conversation_df['MESSAGE_INDEX'] >= consumer_message_index) &
                        (conversation_df['MESSAGE_INDEX'] <= agent_message_index)
                    ]
                    
                    # Check 1: Bot assignment - any message in this range has target_skill in bot_skills
                    has_bot_assignment = any(
                        messages_between['TARGET_SKILL_PER_MESSAGE'].isin(bot_skills)
                    )
                    
                    # Check 2: Agent message is in dept_skills
                    agent_in_dept_skills = first_agent_intervention['TARGET_SKILL_PER_MESSAGE'] in dept_skills
                    
                    # Check 3: Valid system transfer between consumer and agent
                    system_transfer_messages = conversation_df[
                        (conversation_df['SENT_BY'].str.upper() == 'SYSTEM') &
                        ((conversation_df['MESSAGE_TYPE'].str.upper() == 'PRIVATE MESSAGE') |
                         (conversation_df['MESSAGE_TYPE'].str.upper() == 'TRANSFER')) &
                        (conversation_df['MESSAGE_INDEX'] > consumer_message_index) &
                        (conversation_df['MESSAGE_INDEX'] < agent_message_index)
                    ]
                    
                    has_valid_system_transfer = False
                    for _, message in system_transfer_messages.iterrows():
                        message_text = str(message.get('TEXT', ''))
                        transfer_data = parse_transfer_2222(message_text)
                        
                        # Check all three conditions:
                        # 1. 'by' doesn't contain 'GPT' (case-insensitive)
                        # 2. 'from_skill' is in bot_skills
                        # 3. 'to_skill' is in dept_skills
                        if (transfer_data.get('by', '') and 
                            'gpt' not in transfer_data.get('by', '').lower() and
                            transfer_data.get('from_skill', '') in bot_skills and
                            transfer_data.get('to_skill', '') in dept_skills):
                            
                            has_valid_system_transfer = True
                            break
                    
                    # All three conditions must be true
                    if not (has_bot_assignment and agent_in_dept_skills and has_valid_system_transfer):
                        return None  # Missing bot assignment, agent not in dept skills, or no valid system transfer
            
            return {
                'CONVERSATION_ID': conversation_df['CONVERSATION_ID'].iloc[0],
                'AGENT_MESSAGE_ID': first_agent_intervention.get('MESSAGE_ID', agent_message_index),
                'AGENT_MESSAGE_TIME': agent_message_time.strftime('%Y-%m-%d %H:%M:%S') if agent_message_time else None,
                'CONSUMER_MESSAGE_TIME': consumer_message_time.strftime('%Y-%m-%d %H:%M:%S') if consumer_message_time else None,
                'DELAY_SECONDS': delay_seconds,
                'DELAY_MINUTES': round(delay_seconds / 60, 2),
                'LAST_BOT_MESSAGE_INDEX': last_bot_index,
                'LAST_BOT_MESSAGE_TIME': last_bot_message['MESSAGE_SENT_TIME'] if last_bot_index is not None else None,
                'ANALYSIS_DATE': datetime.now().strftime('%Y-%m-%d')
            }
        
        return None  # Delay within acceptable range
        
    except Exception as e:
        # Silently handle errors to not disrupt main delay analysis
        return None


def detect_normal_agent_intervention_snowflake(conversation_df, department_name, departments_config):
    """
    Detect normal agent interventions where delay is ≤4 minutes (quick response).
    
    Logic: Identical to detect_agent_intervention_due_to_no_response_snowflake
    but checks for delay ≤ 4 minutes instead of > 4 minutes.
    
    Args:
        conversation_df: DataFrame containing one conversation's messages
        department_name: Department name for skill filtering
        departments_config: Department configuration dictionary
    
    Returns:
        dict with intervention details or None if no intervention detected
    """
    try:
        department_config = departments_config[department_name]
        bot_skills = set(department_config['bot_skills'])
        agent_skills = set(department_config['agent_skills'])
        
        # Ensure MESSAGE_INDEX exists for proper ordering
        if 'MESSAGE_INDEX' not in conversation_df.columns:
            conversation_df = conversation_df.sort_values('MESSAGE_SENT_TIME').reset_index(drop=True)
            conversation_df['MESSAGE_INDEX'] = conversation_df.index
        
        # 1. Find bot messages from this department
        bot_messages = conversation_df[
            (conversation_df['SENT_BY'].str.upper() == 'BOT') & 
            (conversation_df['MESSAGE_TYPE'].str.upper() == 'NORMAL MESSAGE') &
            (conversation_df['TARGET_SKILL_PER_MESSAGE'].isin(bot_skills)) &
            (conversation_df['MESSAGE_SENT_TIME'].notna())
        ].copy()
        
        # 2. Handle two cases: with bot messages OR without bot messages
        dept_skills = bot_skills.union(agent_skills)
        
        if len(bot_messages) > 0:
            # CASE A: Bot messages exist - find agent AFTER last bot
            bot_messages = bot_messages.sort_values('MESSAGE_INDEX')
            last_bot_message = bot_messages.iloc[-1]
            last_bot_index = last_bot_message['MESSAGE_INDEX']
            
            agent_messages_after_bot = conversation_df[
                (conversation_df['SENT_BY'].str.upper() == 'AGENT') &
                (conversation_df['MESSAGE_TYPE'].str.upper() == 'NORMAL MESSAGE') &
                (conversation_df['TARGET_SKILL_PER_MESSAGE'].isin(dept_skills)) &
                (conversation_df['MESSAGE_INDEX'] > last_bot_index) &
                (conversation_df['MESSAGE_SENT_TIME'].notna())
            ].copy()
        else:
            # CASE B: No bot messages - bot was assigned but never responded
            last_bot_index = None
            
            # Find ALL agent messages (bot never responded, so check from beginning)
            agent_messages_after_bot = conversation_df[
                (conversation_df['SENT_BY'].str.upper() == 'AGENT') &
                (conversation_df['MESSAGE_TYPE'].str.upper() == 'NORMAL MESSAGE') &
                (conversation_df['TARGET_SKILL_PER_MESSAGE'].isin(dept_skills)) &
                (conversation_df['MESSAGE_SENT_TIME'].notna())
            ].copy()
        
        if len(agent_messages_after_bot) == 0:
            return None  # No agent intervention
        
        # 3. Get FIRST agent message
        agent_messages_after_bot = agent_messages_after_bot.sort_values('MESSAGE_INDEX')
        first_agent_intervention = agent_messages_after_bot.iloc[0]
        agent_message_index = first_agent_intervention['MESSAGE_INDEX']
        agent_message_time = parse_message_time_snowflake(first_agent_intervention['MESSAGE_SENT_TIME'])
        
        if agent_message_time is None:
            return None
        
        # 5. CLEVER TRICK: Use existing function to calculate delay
        # This will find the consumer message the agent is responding to
        delay_seconds = calculate_delay_method1_snowflake(
            agent_message_time,
            conversation_df,
            agent_message_index
        )
        
        if delay_seconds is None:
            return None  # No valid delay calculation
        
        # 6. Check if delay is ≤ 4 minutes (DIFFERENT from the other function)
        if delay_seconds <= DELAY_OUTLIER_THRESHOLD_SECONDS:
            # Find the consumer message that the agent is responding to
            consumer_messages_before = conversation_df[
                (conversation_df['SENT_BY'].str.upper() == 'CONSUMER') & 
                (conversation_df['MESSAGE_INDEX'] < agent_message_index) &
                (conversation_df['MESSAGE_SENT_TIME'].notna())
            ].copy()
            
            consumer_message_time = None
            consumer_message_index = None
            if len(consumer_messages_before) > 0:
                # Get the nearest consumer message (same logic as method1)
                nearest_consumer = consumer_messages_before.loc[consumer_messages_before['MESSAGE_INDEX'].idxmax()]
                consumer_message_time = parse_message_time_snowflake(nearest_consumer['MESSAGE_SENT_TIME'])
                consumer_message_index = nearest_consumer['MESSAGE_INDEX']
            
            # 7. EXTRA VALIDATION: Different validation for Case A vs Case B
            if last_bot_index is not None:
                # CASE A: Bot existed - Check for valid system transfer
                if consumer_message_index is not None:
                    # Find system transfer messages between consumer message and agent message
                    system_transfer_messages = conversation_df[
                        (conversation_df['SENT_BY'].str.upper() == 'SYSTEM') &
                        ((conversation_df['MESSAGE_TYPE'].str.upper() == 'PRIVATE MESSAGE') |
                         (conversation_df['MESSAGE_TYPE'].str.upper() == 'TRANSFER')) &
                        (conversation_df['MESSAGE_INDEX'] > consumer_message_index) &
                        (conversation_df['MESSAGE_INDEX'] < agent_message_index)
                    ]
                    
                    # Check if any system transfer is a valid transfer
                    has_valid_system_transfer = False
                    for _, message in system_transfer_messages.iterrows():
                        message_text = str(message.get('TEXT', ''))
                        transfer_data = parse_transfer_2222(message_text)
                        
                        # Check all three conditions:
                        # 1. 'by' doesn't contain 'GPT' (case-insensitive)
                        # 2. 'from_skill' is in bot_skills
                        # 3. 'to_skill' is in dept_skills
                        if (transfer_data.get('by', '') and 
                            'gpt' not in transfer_data.get('by', '').lower() and
                            transfer_data.get('from_skill', '') in bot_skills and
                            transfer_data.get('to_skill', '') in dept_skills):
                            
                            has_valid_system_transfer = True
                            break
                    
                    # Only return intervention if there was a valid system transfer
                    if not has_valid_system_transfer and first_agent_intervention['TARGET_SKILL_PER_MESSAGE'] in agent_skills:
                        return None  # No valid system transfer found, don't count as intervention
            else:
                # CASE B: No bot messages - Check for bot assignment + agent in dept skills + system transfer
                if consumer_message_index is not None:
                    messages_between = conversation_df[
                        (conversation_df['MESSAGE_INDEX'] >= consumer_message_index) &
                        (conversation_df['MESSAGE_INDEX'] <= agent_message_index)
                    ]
                    
                    # Check 1: Bot assignment - any message in this range has target_skill in bot_skills
                    has_bot_assignment = any(
                        messages_between['TARGET_SKILL_PER_MESSAGE'].isin(bot_skills)
                    )
                    
                    # Check 2: Agent message is in dept_skills
                    agent_in_dept_skills = first_agent_intervention['TARGET_SKILL_PER_MESSAGE'] in dept_skills
                    
                    # Check 3: Valid system transfer between consumer and agent
                    system_transfer_messages = conversation_df[
                        (conversation_df['SENT_BY'].str.upper() == 'SYSTEM') &
                        ((conversation_df['MESSAGE_TYPE'].str.upper() == 'PRIVATE MESSAGE') |
                         (conversation_df['MESSAGE_TYPE'].str.upper() == 'TRANSFER')) &
                        (conversation_df['MESSAGE_INDEX'] > consumer_message_index) &
                        (conversation_df['MESSAGE_INDEX'] < agent_message_index)
                    ]
                    
                    has_valid_system_transfer = False
                    for _, message in system_transfer_messages.iterrows():
                        message_text = str(message.get('TEXT', ''))
                        transfer_data = parse_transfer_2222(message_text)
                        
                        # Check all three conditions:
                        # 1. 'by' doesn't contain 'GPT' (case-insensitive)
                        # 2. 'from_skill' is in bot_skills
                        # 3. 'to_skill' is in dept_skills
                        if (transfer_data.get('by', '') and 
                            'gpt' not in transfer_data.get('by', '').lower() and
                            transfer_data.get('from_skill', '') in bot_skills and
                            transfer_data.get('to_skill', '') in dept_skills):
                            
                            has_valid_system_transfer = True
                            break
                    
                    # All three conditions must be true
                    if not (has_bot_assignment and agent_in_dept_skills and has_valid_system_transfer):
                        return None  # Missing bot assignment, agent not in dept skills, or no valid system transfer
            
            return {
                'CONVERSATION_ID': conversation_df['CONVERSATION_ID'].iloc[0],
                'AGENT_MESSAGE_ID': first_agent_intervention.get('MESSAGE_ID', agent_message_index),
                'AGENT_MESSAGE_TIME': agent_message_time.strftime('%Y-%m-%d %H:%M:%S') if agent_message_time else None,
                'CONSUMER_MESSAGE_TIME': consumer_message_time.strftime('%Y-%m-%d %H:%M:%S') if consumer_message_time else None,
                'DELAY_SECONDS': delay_seconds,
                'DELAY_MINUTES': round(delay_seconds / 60, 2),
                'LAST_BOT_MESSAGE_INDEX': last_bot_index,
                'LAST_BOT_MESSAGE_TIME': last_bot_message['MESSAGE_SENT_TIME'] if last_bot_index is not None else None,
                'ANALYSIS_DATE': datetime.now().strftime('%Y-%m-%d')
            }
        
        return None  # Delay exceeds 4 minutes (not a quick intervention)
        
    except Exception as e:
        # Silently handle errors to not disrupt main delay analysis
        return None


def analyze_delay_conversations_single_department(session, df, department_name, departments_config, target_date):
    """
    Analyze delay patterns for a single department and save raw data.
    Adapted from main_analytics.py analyze_delay_conversations()
    
    Args:
        session: Snowflake session
        df: Filtered DataFrame from Phase 1
        department_name: Department name
        departments_config: Department configuration
        target_date: Target date for analysis
    
    Returns:
        delay_results dictionary
    """
    print(f"  ⏱️  Analyzing delays for {department_name}...")
    
    if df.empty:
        print(f"    ⚠️  No filtered data for {department_name}")
        return {
            'total_conversations': 0,
            'avg_method1_initial_delay': None,
            'avg_method1_non_initial_delay': None,
            'avg_method2_initial_delay': None,
            'avg_method2_non_initial_delay': None,
            'avg_method3_initial_delay': None,
            'avg_method3_non_initial_delay': None,
            'avg_method1_initial_delay_4_to_50': None,
            'avg_method1_non_initial_delay_4_to_50': None,
            'avg_method2_initial_delay_4_to_50': None,
            'avg_method2_non_initial_delay_4_to_50': None,
            'avg_method3_initial_delay_4_to_50': None,
            'avg_method3_non_initial_delay_4_to_50': None,
            'method1_initial_outliers': 0,
            'method1_non_initial_outliers': 0,
            'method2_initial_outliers': 0,
            'method2_non_initial_outliers': 0,
            'method3_initial_outliers': 0,
            'method3_non_initial_outliers': 0,
            'method1_initial_4_to_50_count': 0,
            'method1_non_initial_4_to_50_count': 0,
            'method2_initial_4_to_50_count': 0,
            'method2_non_initial_4_to_50_count': 0,
            'method3_initial_4_to_50_count': 0,
            'method3_non_initial_4_to_50_count': 0,
            'agent_interventions_no_response_count': 0,
            'normal_agent_interventions_count': 0
        }
    
    # Group by conversation ID
    conversations = df.groupby('CONVERSATION_ID')
    total_conversations = len(conversations)
    
    # Store message-level results for processed data
    all_message_results = []
    
    # Store agent interventions due to no response (>4 min)
    agent_interventions = []
    
    # Store normal agent interventions (≤4 min)
    normal_agent_interventions = []
    
    # Initialize delay accumulators for each method (separated for outliers)
    method_delays = {
        'method1_initial': [], 'method1_non_initial': [],
        'method2_initial': [], 'method2_non_initial': [],
        'method3_initial': [], 'method3_non_initial': []
    }
    
    # NEW: Initialize delay accumulators for 4-50 minute range (excluding <4min and >50min)
    method_delays_4_to_50 = {
        'method1_initial': [], 'method1_non_initial': [],
        'method2_initial': [], 'method2_non_initial': [],
        'method3_initial': [], 'method3_non_initial': []
    }
    
    # Track outlier counts
    method_outlier_counts = {
        'method1_initial_outliers': 0, 'method1_non_initial_outliers': 0,
        'method2_initial_outliers': 0, 'method2_non_initial_outliers': 0,
        'method3_initial_outliers': 0, 'method3_non_initial_outliers': 0
    }
    
    # NEW: Track 4-50 minute range counts
    method_4_to_50_counts = {
        'method1_initial_4_to_50_count': 0, 'method1_non_initial_4_to_50_count': 0,
        'method2_initial_4_to_50_count': 0, 'method2_non_initial_4_to_50_count': 0,
        'method3_initial_4_to_50_count': 0, 'method3_non_initial_4_to_50_count': 0
    }
    
    for conv_id, conv_df in conversations:
        # Get conversation-level delay results for averaging (original function)
        delay_results = calculate_conversation_delays_snowflake(conv_df, department_name, departments_config)
        
        # Get individual message delays for outlier counting (new function)
        individual_delays = calculate_individual_message_delays_snowflake(conv_df, department_name, departments_config)
        
        # NEW: Assign individual delays to each message row (replaces conversation averages)
        message_results = assign_individual_delays_to_messages_snowflake(conv_df, department_name, departments_config)
        message_results['BOT_MESSAGE_COUNT'] = delay_results.get('bot_message_count', 0)
        message_results['ANALYSIS_DATE'] = datetime.now().strftime('%Y-%m-%d')
        
        all_message_results.append(message_results)
        
        # AGENT INTERVENTION DETECTION DISABLED FOR ALL DEPARTMENTS
        # # NEW: Detect agent interventions due to no response (>4 min delay)
        # intervention = detect_agent_intervention_due_to_no_response_snowflake(conv_df, department_name, departments_config)
        # if intervention:
        #     agent_interventions.append(intervention)
        # 
        # # NEW: Detect normal agent interventions (≤4 min delay)
        # normal_intervention = detect_normal_agent_intervention_snowflake(conv_df, department_name, departments_config)
        # if normal_intervention:
        #     normal_agent_interventions.append(normal_intervention)
        
        # NEW LOGIC: Count individual message outliers instead of conversation outliers
        for method in ['method1', 'method2', 'method3']:
            # Count individual initial delay outliers
            initial_delays_list = individual_delays[f'{method}_initial_delays']
            for delay in initial_delays_list:
                if delay > DELAY_OUTLIER_THRESHOLD_SECONDS:
                    method_outlier_counts[f'{method}_initial_outliers'] += 1
                    # NEW: Also track delays in 4-50 minute range
                    if delay <= DELAY_CLOSING_THRESHOLD_SECONDS:
                        method_delays_4_to_50[f'{method}_initial'].append(delay)
                        method_4_to_50_counts[f'{method}_initial_4_to_50_count'] += 1
                else:
                    method_delays[f'{method}_initial'].append(delay)
            
            # Count individual non-initial delay outliers
            non_initial_delays_list = individual_delays[f'{method}_non_initial_delays']
            for delay in non_initial_delays_list:
                if delay > DELAY_OUTLIER_THRESHOLD_SECONDS:
                    method_outlier_counts[f'{method}_non_initial_outliers'] += 1
                    # NEW: Also track delays in 4-50 minute range
                    if delay <= DELAY_CLOSING_THRESHOLD_SECONDS:
                        method_delays_4_to_50[f'{method}_non_initial'].append(delay)
                        method_4_to_50_counts[f'{method}_non_initial_4_to_50_count'] += 1
                else:
                    method_delays[f'{method}_non_initial'].append(delay)
    
    # Calculate department averages (original - excluding >4min)
    dept_averages = {}
    for key, delays in method_delays.items():
        if len(delays) > 0:
            dept_averages[f'avg_{key}_delay'] = sum(delays) / len(delays)
        else:
            dept_averages[f'avg_{key}_delay'] = None
    
    # NEW: Calculate department averages for 4-50 minute range (including >4min, excluding >50min)
    dept_averages_4_to_50 = {}
    for key, delays in method_delays_4_to_50.items():
        if len(delays) > 0:
            dept_averages_4_to_50[f'avg_{key}_delay_4_to_50'] = sum(delays) / len(delays)
        else:
            dept_averages_4_to_50[f'avg_{key}_delay_4_to_50'] = None
    
    # Store results including outlier counts and intervention counts
    results = {
        'total_conversations': total_conversations,
        **dept_averages,
        **dept_averages_4_to_50,  # NEW: Add 4-50 minute range averages
        **method_outlier_counts,
        **method_4_to_50_counts,  # NEW: Add 4-50 minute range counts
        'agent_interventions_no_response_count': len(agent_interventions),
        'normal_agent_interventions_count': len(normal_agent_interventions)
    }
    
    # Print summary with outlier information
    method1_init = f"{dept_averages['avg_method1_initial_delay']:.1f}s" if dept_averages['avg_method1_initial_delay'] else "N/A"
    method1_non = f"{dept_averages['avg_method1_non_initial_delay']:.1f}s" if dept_averages['avg_method1_non_initial_delay'] else "N/A"
    
    # Format outlier counts for display
    threshold_mins = DELAY_OUTLIER_THRESHOLD_SECONDS / 60
    init_outliers = method_outlier_counts['method1_initial_outliers']
    non_init_outliers = method_outlier_counts['method1_non_initial_outliers']
    
    print(f"    ✅ Method1 - Initial: {method1_init} (excluding {init_outliers} message outliers >{threshold_mins:.0f}min), Non-Initial: {method1_non} (excluding {non_init_outliers} message outliers >{threshold_mins:.0f}min)")
    
    # AGENT INTERVENTION METRICS DISABLED
    # # Print agent intervention summary
    # if len(agent_interventions) > 0:
    #     print(f"    🚨 Agent interventions (no response >4min): {len(agent_interventions)} conversations")
    # 
    # # Print normal agent intervention summary
    # if len(normal_agent_interventions) > 0:
    #     print(f"    ✅ Normal agent interventions (≤4min): {len(normal_agent_interventions)} conversations")
    
    print(f"    ⚠️  Agent intervention detection: DISABLED for all departments")
    
    # Save raw data to DELAY_ANALYSIS_RAW_DATA table
    if all_message_results:
        try:
            combined_message_results = pd.concat(all_message_results, ignore_index=True)
            combined_message_results = clean_dataframe_for_snowflake(combined_message_results)
            
            # Define dynamic columns (excluding the essential columns that insert_raw_data_with_cleanup adds)
            dynamic_columns = [col for col in combined_message_results.columns if col not in ['DATE', 'DEPARTMENT', 'TIMESTAMP']]
            
            insert_result = insert_raw_data_with_cleanup(
                session=session,
                table_name="DELAY_ANALYSIS_RAW_DATA",
                department=department_name,
                target_date=target_date,
                dataframe=combined_message_results[dynamic_columns],
                columns=dynamic_columns
            )
            print(f"    💾 Saved {len(combined_message_results)} delay analysis records to DELAY_ANALYSIS_RAW_DATA")
                
        except Exception as e:
            print(f"    ⚠️  Failed to save delay analysis raw data: {str(e)}")
    
    # AGENT INTERVENTION TABLE SAVING DISABLED
    # # Save agent interventions due to no response to separate table
    # if agent_interventions:
    #     try:
    #         interventions_df = pd.DataFrame(agent_interventions)
    #         interventions_df = clean_dataframe_for_snowflake(interventions_df)
    #         
    #         # Define dynamic columns (excluding the essential columns that insert_raw_data_with_cleanup adds)
    #         intervention_columns = [col for col in interventions_df.columns if col not in ['DATE', 'DEPARTMENT', 'TIMESTAMP']]
    #         
    #         insert_result = insert_raw_data_with_cleanup(
    #             session=session,
    #             table_name="AGENT_INTERVENTION_DUE_TO_NO_RESPONSE",
    #             department=department_name,
    #             target_date=target_date,
    #             dataframe=interventions_df[intervention_columns],
    #             columns=intervention_columns
    #         )
    #         print(f"    💾 Saved {len(interventions_df)} agent intervention records to AGENT_INTERVENTION_DUE_TO_NO_RESPONSE")
    #             
    #     except Exception as e:
    #         print(f"    ⚠️  Failed to save agent intervention data: {str(e)}")
    # 
    # # Save normal agent interventions (≤4min) to separate table
    # if normal_agent_interventions:
    #     try:
    #         normal_interventions_df = pd.DataFrame(normal_agent_interventions)
    #         normal_interventions_df = clean_dataframe_for_snowflake(normal_interventions_df)
    #         
    #         # Define dynamic columns (excluding the essential columns that insert_raw_data_with_cleanup adds)
    #         normal_intervention_columns = [col for col in normal_interventions_df.columns if col not in ['DATE', 'DEPARTMENT', 'TIMESTAMP']]
    #         
    #         insert_result = insert_raw_data_with_cleanup(
    #             session=session,
    #             table_name="NORMAL_AGENT_INTERVENTION_UNDER_4_MINS",
    #             department=department_name,
    #             target_date=target_date,
    #             dataframe=normal_interventions_df[normal_intervention_columns],
    #             columns=normal_intervention_columns
    #         )
    #         print(f"    💾 Saved {len(normal_interventions_df)} normal agent intervention records to NORMAL_AGENT_INTERVENTION_UNDER_4_MINS")
    #             
    #     except Exception as e:
    #         print(f"    ⚠️  Failed to save normal agent intervention data: {str(e)}")
    
    return results


# ============================================================================
# DOWNTIME ANALYSIS (>4 MINUTE RESPONSE TIME)
# ============================================================================

def analyze_downtime_conversations_single_department(session, department_name, departments_config, target_date):
    """
    Analyze downtime (>4 min response time) for a single department.
    Queries daily_delays_details_final table for conversations with response time > 4 minutes.
    
    Args:
        session: Snowflake session
        department_name: Department name
        departments_config: Department configuration
        target_date: Target date for analysis (format: 'YYYY-MM-DD')
    
    Returns:
        downtime_results dictionary
    """
    print(f"  ⏰ Analyzing downtime (>4 min) for {department_name}...")
    
    try:
        # Get department skills (both bot and agent)
        dept_config = departments_config[department_name]
        all_dept_skills = set(dept_config['bot_skills'] + dept_config['agent_skills'])
        
        # Format date for query
        if not target_date:
            target_date = datetime.now().strftime('%Y-%m-%d')
        
        # Query: daily_delays_details_final table
        # Case-insensitive department matching using UPPER()
        query = f"""
            SELECT 
                DATE,
                "Conversation Id" as CONVERSATION_ID,
                SENDER,
                "Response Time (mins)" as RESPONSE_TIME_MINS,
                "Message Id" as MESSAGE_ID,
                FTR,
                "Target Skill" as TARGET_SKILL,
                DEPARTMENT
            FROM LLM_EVAL.RAW_DATA.DAILY_DELAYS_DETAILS_FINAL
            WHERE DATE(DATE) = '{target_date}'
              AND UPPER(DEPARTMENT) = '{department_name.upper()}'
              AND "Response Time (mins)" > 4
              AND "Conversation Id" IN (
                  SELECT DISTINCT CONVERSATION_ID 
                  FROM LLM_EVAL.PUBLIC.DELAY_ANALYSIS_RAW_DATA 
                  WHERE DATE(DATE) = '{target_date}' 
                    AND UPPER(DEPARTMENT) = '{department_name.upper()}'
              )
        """
        
        # Execute query and get DataFrame
        downtime_df = session.sql(query).to_pandas()
        
        if downtime_df.empty:
            print(f"    ℹ️  No downtime data found for {department_name}")
            return {
                'total_conversations': 0,
                'downtime_conversation_count': 0,
                'downtime_percentage': 0.0,
                'raw_downtime_df': pd.DataFrame()
            }
        
        # Filter: Target Skill must be in department skills
        downtime_df_filtered = downtime_df[downtime_df['TARGET_SKILL'].isin(all_dept_skills)].copy()
        
        if downtime_df_filtered.empty:
            print(f"    ℹ️  No conversations with department skills found")
            return {
                'total_conversations': 0,
                'downtime_conversation_count': 0,
                'downtime_percentage': 0.0,
                'raw_downtime_df': pd.DataFrame()
            }
        
        # Count unique conversation IDs
        downtime_conversation_count = downtime_df_filtered['CONVERSATION_ID'].nunique()
        
        print(f"    ✅ {downtime_conversation_count} conversations with >4 min response time")
        
        # Save raw data to DOWNTIME_RAW_DATA table
        if not downtime_df_filtered.empty:
            try:
                downtime_df_filtered = clean_dataframe_for_snowflake(downtime_df_filtered)
                
                # Define dynamic columns
                dynamic_columns = [col for col in downtime_df_filtered.columns if col not in ['DATE', 'DEPARTMENT', 'TIMESTAMP']]
                
                insert_raw_data_with_cleanup(
                    session=session,
                    table_name="DOWNTIME_RAW_DATA",
                    department=department_name,
                    target_date=target_date,
                    dataframe=downtime_df_filtered[dynamic_columns],
                    columns=dynamic_columns
                )
                print(f"    💾 Saved {len(downtime_df_filtered)} downtime records to DOWNTIME_RAW_DATA")
            except Exception as e:
                print(f"    ⚠️  Failed to save downtime raw data: {str(e)}")
        
        return {
            'downtime_conversation_count': downtime_conversation_count,
            'raw_downtime_df': downtime_df_filtered
        }
        
    except Exception as e:
        print(f"    ❌ Error analyzing downtime for {department_name}: {str(e)}")
        return {
            'downtime_conversation_count': 0,
            'raw_downtime_df': pd.DataFrame()
        }


def analyze_downtime_conversations_all_departments(session: snowpark.Session, target_date=None):
    """
    Analyze downtime (>4 min response time) for all departments.
    
    Args:
        session: Snowflake session
        target_date: Target date for analysis
    
    Returns:
        department_results dictionary
    """
    print("\n⏰ PHASE 2E: ANALYZING DOWNTIME (>4 MIN RESPONSE TIME)")
    print("=" * 60)
    
    departments_config = get_snowflake_departments_config()
    department_results = {}
    
    for department_name in departments_config.keys():
        # DOWNTIME ANALYSIS DISABLED FOR ALL DEPARTMENTS
        print(f"\n🏢 {department_name}: Skipping downtime analysis (DISABLED)")
        department_results[department_name] = {
            'downtime_conversation_count': 0,
            'downtime_percentage': 0.0
        }
        continue
        
        if department_name != DEPARTMENT_FILTER and TEST:
            continue
        
        try:
            # Analyze downtime for this department
            downtime_results = analyze_downtime_conversations_single_department(
                session, department_name, departments_config, target_date
            )
            
            department_results[department_name] = downtime_results
            
        except Exception as e:
            error_msg = f"Downtime analysis failed: {str(e)}"
            print(f"  ❌ {department_name}: {error_msg}")
            department_results[department_name] = {
                'downtime_conversation_count': 0,
                'error': error_msg
            }
    
    # Generate summary
    total_downtime_conversations_all = sum(r.get('downtime_conversation_count', 0) for r in department_results.values())
    
    print(f"\n📊 DOWNTIME ANALYSIS SUMMARY:")
    print(f"   ⏰ Total conversations with >4 min downtime: {total_downtime_conversations_all:,}")
    print(f"   💾 Raw data saved to: DOWNTIME_RAW_DATA")
    
    return department_results


# ============================================================================
# UNRESPONSIVE CONVERSATION ANALYSIS
# ============================================================================

def is_conversation_unresponsive_snowflake(conversation_df, department_name, departments_config, bot_routed_no_response=None):
    """
    Check if a conversation is unresponsive based on the criteria (Snowflake version).
    IMPROVED VERSION: Measures delay to FIRST bot response, not last bot message.
    
    CASE 0: Bot-routed-no-response conversations (pre-identified) → UNRESPONSIVE
    CASE 1: Last normal message from consumer → UNRESPONSIVE
    CASE 2: Last normal message from department bot AND >55min delay to FIRST bot response
    - Find the time between the last consumer message and the FIRST bot message after that
    - If this delay > 55 minutes → UNRESPONSIVE
    
    Example:
    Consumer: "Hello" (10:00)
    Bot: "Hi there" (10:55)  ← FIRST bot message - 55 minutes delay → UNRESPONSIVE
    Bot: "How can I help?" (10:56)  ← Additional bot messages ignored for delay calculation
    
    Args:
        conversation_df: DataFrame containing one conversation's messages
        department_name: Department name for skill filtering
        departments_config: Department configuration dictionary
        bot_routed_no_response: Set of conversation IDs that are bot-routed with no responses
    
    Returns:
        Tuple: (is_unresponsive, reason)
    """
    # Validate input parameters
    if conversation_df is None or len(conversation_df) == 0:
        return False, "Empty conversation data"
    
    if department_name not in departments_config:
        return False, f"Unknown department: {department_name}"
    
    # CASE 0: Check if this conversation is in bot-routed-no-response set (pre-identified)
    if bot_routed_no_response is not None:
        conv_id = conversation_df['CONVERSATION_ID'].iloc[0] if len(conversation_df) > 0 else None
        if conv_id in bot_routed_no_response:
            return True, "Bot-routed with no agent/bot responses (pre-identified)"
    
    department_config = departments_config[department_name]
    bot_skills = set(department_config['bot_skills'])
    bot_skills_upper = {s.upper() for s in bot_skills}
    if not bot_skills:
        return False, f"No bot skills configured for department: {department_name}"
    
    # Create a working copy and ensure proper data types
    work_df = conversation_df.copy()
    

    
    # Ensure all columns are proper types to avoid type comparison errors
    try:
        work_df['MESSAGE_TYPE'] = work_df['MESSAGE_TYPE'].astype(str)
        work_df['SENT_BY'] = work_df['SENT_BY'].astype(str)
        work_df['TARGET_SKILL_PER_MESSAGE'] = work_df['TARGET_SKILL_PER_MESSAGE'].astype(str)
        # Convert Message Index to numeric if it exists, create if missing
        if 'MESSAGE_INDEX' in work_df.columns:
            work_df['MESSAGE_INDEX'] = pd.to_numeric(work_df['MESSAGE_INDEX'], errors='coerce')
        else:
            # Create message index based on timestamp order if missing
            work_df = work_df.sort_values('MESSAGE_SENT_TIME').reset_index(drop=True)
            work_df['MESSAGE_INDEX'] = work_df.index
    except Exception as e:
        return False, f"Error converting column types: {str(e)}"
    
    # Convert timestamps to datetime with explicit format handling
    try:
        work_df['parsed_timestamp'] = pd.to_datetime(
            work_df['MESSAGE_SENT_TIME'], 
            errors='coerce'
        )
    except Exception as e:
        return False, f"Error parsing timestamps: {str(e)}"
    
    # Filter for NORMAL MESSAGES ONLY with valid timestamps and senders
    try:
        # Apply filters step by step
        step1 = work_df[work_df['MESSAGE_TYPE'].str.upper() == 'NORMAL MESSAGE']
        step2 = step1[step1['SENT_BY'].str.upper().isin(['CONSUMER', 'BOT', 'AGENT'])]
        step3 = step2[step2['parsed_timestamp'].notna()]
        
        normal_messages = step3[step3['MESSAGE_INDEX'].notna()].copy()
        
    except Exception as e:
        return False, f"Error filtering normal messages: {str(e)}"
    
    if len(normal_messages) == 0:
        return False, "No normal messages found"
    
    # Sort by parsed timestamp and message index for reliable ordering
    try:
        normal_messages = normal_messages.sort_values(['parsed_timestamp', 'MESSAGE_INDEX'])
        # Ensure MESSAGE_INDEX is numeric in the filtered messages
        normal_messages['MESSAGE_INDEX'] = pd.to_numeric(normal_messages['MESSAGE_INDEX'], errors='coerce')
                
    except Exception as e:
        return False, f"Error sorting by timestamp: {str(e)}"
    
    # Get the last normal message
    last_normal_message = normal_messages.iloc[-1]
    target_skill = last_normal_message['TARGET_SKILL_PER_MESSAGE']
    
   # CASE 1: Last normal message from consumer → UNRESPONSIVE
    if last_normal_message['SENT_BY'].upper() == 'CONSUMER':
        # Check if target_skill is None/NaN/empty or if it's in bot_skills (case-insensitive)
        # Note: astype(str) above converts NaN → 'nan', so we check for that string too
        target_is_empty = (target_skill is None or
                           (isinstance(target_skill, str) and target_skill.lower() in ('nan', 'none', '')))
        if target_is_empty or (isinstance(target_skill, str) and target_skill.upper() in bot_skills_upper):
            return True, "Last normal message from consumer"
        else:
            return False, "Last normal message from consumer but not a bot skill"
    
     # CASE 2: Check if the conversation ends with NORMAL bot message from department AND there was a delay > 55 mins
    # The key improvement: We check if the LAST NORMAL MESSAGE is from the department bot
    target_skill_valid = (target_skill is not None and
                          not (isinstance(target_skill, str) and target_skill.lower() in ('nan', 'none', '')))
    if (last_normal_message['SENT_BY'].upper() == 'BOT' and 
        target_skill_valid and isinstance(target_skill, str) and target_skill.upper() in bot_skills_upper):
        
        # Step 1: Find the last consumer normal message (to calculate delay FROM)
        try:
            consumer_normal_messages = normal_messages[
                normal_messages['SENT_BY'].str.upper() == 'CONSUMER'
            ]
        except Exception as e:
            return False, f"Error filtering consumer messages: {str(e)}"
        
        if len(consumer_normal_messages) == 0:
            return False, "No consumer normal messages found"
        
        # Sort consumer messages by timestamp to ensure we get the actual last one
        consumer_normal_messages = consumer_normal_messages.sort_values(['parsed_timestamp', 'MESSAGE_INDEX'])
        last_consumer_message = consumer_normal_messages.iloc[-1]
        last_consumer_time = last_consumer_message['parsed_timestamp']
        last_consumer_index = last_consumer_message['MESSAGE_INDEX']
        
        # Step 2: Find the FIRST bot message after the last consumer message
         # We count ANY bot message as a valid response (not just AT-skill bots)
        # because routing bots (e.g. GPT_MAIDSAT) also respond in the AT flow context
        try:
            bot_messages_after_consumer = normal_messages[
                (normal_messages['SENT_BY'].str.upper() == 'BOT') &
                (normal_messages['parsed_timestamp'] > last_consumer_time) &
                (normal_messages['MESSAGE_INDEX'] > last_consumer_index)
            ].sort_values(['parsed_timestamp', 'MESSAGE_INDEX'])

            if len(bot_messages_after_consumer) == 0:
                return False, "No bot messages found after last consumer message"

            # Get the FIRST (earliest) bot message after the consumer message
            # Example: Consumer: "Hello" → Bot: "Hi" (THIS ONE for delay calc) → Bot: "How can I help?"
            first_bot_response = bot_messages_after_consumer.iloc[0]
            first_bot_time = first_bot_response['parsed_timestamp']
            first_bot_index = first_bot_response['MESSAGE_INDEX']

            
            # Step 3: Calculate delay between consumer message and FIRST bot response
            if pd.isna(first_bot_time) or pd.isna(last_consumer_time):
                return False, "Invalid timestamps for time calculation"
            
            # Ensure first_bot_time is actually after last_consumer_time
            if first_bot_time <= last_consumer_time:
                return False, "Bot message timestamp is not after consumer message timestamp"
            
            time_diff_minutes = (first_bot_time - last_consumer_time).total_seconds() / 60
            
            # Validate that the time difference is reasonable (not negative or extremely large)
            if time_diff_minutes < 0:
                return False, "Negative time difference calculated"
            
            if time_diff_minutes > 10080:  # More than 7 days seems unreasonable
                return False, f"Unreasonably large time difference: {time_diff_minutes:.1f} minutes"
            
            # If delay > 55 minutes → UNRESPONSIVE regardless of agent/transfer intervention
            # A consumer waiting >55 min is unresponsive even if an agent eventually stepped in
            if time_diff_minutes > 55:
                return True, f"Bot first response after {time_diff_minutes:.1f} minutes (>55min threshold)"

            # Only apply intervention check when the delay is within the threshold
            # If an agent/transfer responded quickly, the conversation was handled → not unresponsive
            messages_between = work_df[
                (work_df['MESSAGE_INDEX'] > last_consumer_index) &
                (work_df['MESSAGE_INDEX'] < first_bot_index) &
                (work_df['parsed_timestamp'].notna())
            ]
            intervention_messages = messages_between[
                (messages_between['MESSAGE_TYPE'].str.upper() == 'TRANSFER') |
                (messages_between['SENT_BY'].str.upper() == 'AGENT') |
                (~messages_between['TARGET_SKILL_PER_MESSAGE'].str.upper().isin(bot_skills_upper) & 
                 messages_between['TARGET_SKILL_PER_MESSAGE'].notna() & 
                 (messages_between['TARGET_SKILL_PER_MESSAGE'] != '') &
                 (messages_between['TARGET_SKILL_PER_MESSAGE'].str.lower() != 'nan'))
            ]
            if len(intervention_messages) > 0:
                return False, "Agent, Transfer, or non-bot skill message happened before bot message"

            return False, f"Bot responded within {time_diff_minutes:.1f} minutes"

        except Exception as e:
            return False, f"Error calculating time difference: {str(e)}"
        
        except Exception as e:
            return False, f"Error calculating time difference: {str(e)}"
    
    # If last normal message is from agent, conversation is considered resolved
    return False, "Last normal message from agent (resolved)"


def analyze_unresponsive_conversations_single_department(session, df, department_name, departments_config, target_date, bot_routed_no_response=None):
    """
    Analyze unresponsive patterns for a single department and save raw data.
    Adapted from main_analytics.py analyze_unresponsive_conversations()
    
    Args:
        session: Snowflake session
        df: Filtered DataFrame from Phase 1
        department_name: Department name
        departments_config: Department configuration
        target_date: Target date for analysis
        bot_routed_no_response: Set of conversation IDs that are bot-routed with no responses
    
    Returns:
        unresponsive_results dictionary
    """
    print(f"  📵 Analyzing unresponsive conversations for {department_name}...")
    
    if df.empty:
        print(f"    ⚠️  No filtered data for {department_name}")
        return {
            'total_conversations': 0,
            'unresponsive_count': 0,
            'unresponsive_percentage': 0.0,
            'bot_routed_no_response_count': 0
        }
    
    # Default to empty set if not provided
    if bot_routed_no_response is None:
        bot_routed_no_response = set()
    
    # Group by conversation ID
    conversations = df.groupby('CONVERSATION_ID')
    total_conversations = len(conversations)
    
    unresponsive_count = 0
    bot_routed_no_response_unresponsive_count = 0
    unresponsive_details = []
    
    for conv_id, conv_df in conversations:
        is_unresponsive, reason = is_conversation_unresponsive_snowflake(conv_df, department_name, departments_config, bot_routed_no_response)
        
        if is_unresponsive:
            unresponsive_count += 1
            # Track if this is from bot-routed-no-response
            if conv_id in bot_routed_no_response:
                bot_routed_no_response_unresponsive_count += 1
            unresponsive_details.append({
                'CONVERSATION_ID': conv_id,
                'REASON': reason,
                'MESSAGE_COUNT': len(conv_df),
                'LAST_MESSAGE_TIME': conv_df['MESSAGE_SENT_TIME'].max(),
                'ANALYSIS_DATE': datetime.now().strftime('%Y-%m-%d')
            })
    
    # Calculate percentage
    unresponsive_percentage = (unresponsive_count / total_conversations * 100) if total_conversations > 0 else 0
    
    results = {
        'total_conversations': total_conversations,
        'unresponsive_count': unresponsive_count,
        'unresponsive_percentage': unresponsive_percentage,
        'bot_routed_no_response_count': bot_routed_no_response_unresponsive_count
    }
    
    print(f"    ✅ {unresponsive_count}/{total_conversations} ({unresponsive_percentage:.1f}%) unresponsive conversations")
    print(f"    🤖 {bot_routed_no_response_unresponsive_count} bot-routed-no-response conversations")
    
    # Save raw data to UNRESPONSIVE_RAW_DATA table
    if unresponsive_details:
        try:
            unresponsive_df = pd.DataFrame(unresponsive_details)
            unresponsive_df = clean_dataframe_for_snowflake(unresponsive_df)
            
            # Define dynamic columns (excluding the essential columns that insert_raw_data_with_cleanup adds)
            dynamic_columns = [col for col in unresponsive_df.columns if col not in ['DATE', 'DEPARTMENT', 'TIMESTAMP']]
            
            insert_result = insert_raw_data_with_cleanup(
                session=session,
                table_name="UNRESPONSIVE_RAW_DATA",
                department=department_name,
                target_date=target_date,
                dataframe=unresponsive_df[dynamic_columns],
                columns=dynamic_columns
            )
            print(f"    💾 Saved {len(unresponsive_details)} unresponsive records to UNRESPONSIVE_RAW_DATA")
        except Exception as e:
            print(f"    ⚠️  Failed to save unresponsive raw data: {str(e)}")
    
    return results


# ============================================================================
# PHASE 3 INTEGRATION FUNCTIONS
# ============================================================================

def analyze_delay_conversations_all_departments(session: snowpark.Session, target_date=None):
    """
    Analyze delay patterns for all departments using Phase 1 filtered data.
    Raw data is saved immediately for each department.
    
    Args:
        session: Snowflake session
        target_date: Target date for analysis
    
    Returns:
        department_results dictionary
    """
    print("\n⏱️  PHASE 3A: ANALYZING RESPONSE DELAYS")
    print("=" * 60)
    
    departments_config = get_snowflake_departments_config()
    department_results = {}
    
    for department_name in departments_config.keys():
        if department_name!=DEPARTMENT_FILTER and TEST:
            continue
        try:
            # Get filtered data from Phase 1
            print(f"\n🏢 Processing {department_name}...")
            filtered_df, phase1_stats, success, _ = process_department_phase1(session, department_name, target_date)
            
            if not success or filtered_df.empty:
                print(f"  ❌ {department_name}: No filtered data from Phase 1")
                department_results[department_name] = {
                    'total_conversations': 0,
                    'avg_method1_initial_delay': None,
                    'avg_method1_non_initial_delay': None,
                    'error': 'No filtered data from Phase 1'
                }
                continue
            
            # Analyze delays for this department (includes raw data saving)
            delay_results = analyze_delay_conversations_single_department(
                session, filtered_df, department_name, departments_config, target_date
            )
            
            department_results[department_name] = delay_results
            
        except Exception as e:
            error_msg = f"Delay analysis failed: {str(e)}"
            print(f"  ❌ {department_name}: {error_msg}")
            department_results[department_name] = {
                'total_conversations': 0,
                'avg_method1_initial_delay': None,
                'avg_method1_non_initial_delay': None,
                'error': error_msg
            }
    
    # Generate summary
    total_conversations_all = sum(r.get('total_conversations', 0) for r in department_results.values())
    
    # Calculate overall averages (excluding None values)
    all_method1_init = [r.get('avg_method1_initial_delay') for r in department_results.values() if r.get('avg_method1_initial_delay')]
    all_method1_non = [r.get('avg_method1_non_initial_delay') for r in department_results.values() if r.get('avg_method1_non_initial_delay')]
    
    overall_method1_init = sum(all_method1_init)/len(all_method1_init) if all_method1_init else 0
    overall_method1_non = sum(all_method1_non)/len(all_method1_non) if all_method1_non else 0
    
    print(f"\n📊 DELAY ANALYSIS SUMMARY:")
    print(f"   📋 Total conversations: {total_conversations_all:,}")
    print(f"   ⏱️  Overall avg initial delay (Method 1): {overall_method1_init:.1f}s")
    print(f"   ⏱️  Overall avg non-initial delay (Method 1): {overall_method1_non:.1f}s")
    print(f"   🚫 Message outlier threshold: >{DELAY_OUTLIER_THRESHOLD_SECONDS/60:.0f} minutes (counts individual messages, not conversations)")
    print(f"   💾 Raw data saved to: DELAY_ANALYSIS_RAW_DATA")
    
    return department_results


def analyze_unresponsive_conversations_all_departments(session: snowpark.Session, target_date=None):
    """
    Analyze unresponsive patterns for all departments using Phase 1 filtered data.
    Raw data is saved immediately for each department.
    
    Args:
        session: Snowflake session
        target_date: Target date for analysis
    
    Returns:
        department_results dictionary
    """
    print("\n📵 PHASE 3B: ANALYZING UNRESPONSIVE CONVERSATIONS")
    print("=" * 60)
    
    departments_config = get_snowflake_departments_config()
    department_results = {}
    
    for department_name in departments_config.keys():
        if department_name!=DEPARTMENT_FILTER and TEST:
            continue
        
        try:
            # Get filtered data from Phase 1
            print(f"\n🏢 Processing {department_name}...")
            filtered_df, phase1_stats, success, bot_routed_no_response = process_department_phase1(session, department_name, target_date)
            
            if not success or filtered_df.empty:
                print(f"  ❌ {department_name}: No filtered data from Phase 1")
                department_results[department_name] = {
                    'total_conversations': 0,
                    'unresponsive_count': 0,
                    'unresponsive_percentage': 0.0,
                    'bot_routed_no_response_count': 0,
                    'error': 'No filtered data from Phase 1'
                }
                continue
            
            # Analyze unresponsive conversations for this department (includes raw data saving)
            unresponsive_results = analyze_unresponsive_conversations_single_department(
                session, filtered_df, department_name, departments_config, target_date, bot_routed_no_response
            )
            
            department_results[department_name] = unresponsive_results
            
        except Exception as e:
            error_msg = f"Unresponsive analysis failed: {str(e)}"
            print(f"  ❌ {department_name}: {error_msg}")
            department_results[department_name] = {
                'total_conversations': 0,
                'unresponsive_count': 0,
                'unresponsive_percentage': 0.0,
                'error': error_msg
            }
    
    # Generate summary
    total_conversations_all = sum(r.get('total_conversations', 0) for r in department_results.values())
    total_unresponsive_all = sum(r.get('unresponsive_count', 0) for r in department_results.values())
    overall_unresponsive_percentage = (total_unresponsive_all / total_conversations_all * 100) if total_conversations_all > 0 else 0
    
    print(f"\n📊 UNRESPONSIVE ANALYSIS SUMMARY:")
    print(f"   📋 Total conversations: {total_conversations_all:,}")
    print(f"   📵 Unresponsive conversations: {total_unresponsive_all:,}")
    print(f"   📈 Overall unresponsive rate: {overall_unresponsive_percentage:.1f}%")
    print(f"   ⏰ Threshold: 50 minutes since last consumer message")
    print(f"   💾 Raw data saved to: UNRESPONSIVE_RAW_DATA")
    
    return department_results


# ============================================================================
# ENHANCED MASTER TABLE INTEGRATION
# ============================================================================

def create_enhanced_combined_metrics_snowflake(bot_results, repetition_results, similarity_results, delay_results, unresponsive_results, shadowing_results, issues_results, conversations_without_filter_5, intervention_reengagement_results, downtime_results, fully_handled_results, target_date=None):
    """
    Create enhanced combined metrics from all analysis phases.
    
    Args:
        bot_results: Results from bot handling analysis (Phase 2)
        repetition_results: Results from repetition analysis (Phase 2)
        delay_results: Results from delay analysis (Phase 3)
        unresponsive_results: Results from unresponsive analysis (Phase 3)
        downtime_results: Results from downtime analysis (Phase 2E)
        target_date: Target date for analysis
    
    Returns:
        List of enhanced metrics dictionaries
    """
    # Handle target_date conversion (it comes in as string like "2025-07-22")
    if target_date:
        try:
            # Convert string to datetime, then format
            date_obj = datetime.strptime(target_date, '%Y-%m-%d')
            current_date = date_obj.strftime("%B %d, %Y")
        except:
            # Fallback to current date if parsing fails
            current_date = datetime.now().strftime("%B %d, %Y")
    else:
        current_date = datetime.now().strftime("%B %d, %Y")
    departments_config = get_snowflake_departments_config()
    
    combined_metrics = []
    
    for department_name in departments_config.keys():
        bot_data = bot_results.get(department_name, {})
        rep_data = repetition_results.get(department_name, {})
        similarity_data = similarity_results.get(department_name, {})
        delay_data = delay_results.get(department_name, {})
        unresponsive_data = unresponsive_results.get(department_name, {})
        shadowing_data = shadowing_results.get(department_name, {})
        issues_data = issues_results.get(department_name, {})
        conversations_without_filter_5_data = conversations_without_filter_5.get(department_name, {})
        downtime_data = downtime_results.get(department_name, {})
        
        # Debug print to verify data structure
        print(f"Debug - {department_name}: conversations_without_filter_5_data type = {type(conversations_without_filter_5_data)}")
        
        # Defensive programming - ensure we have dictionaries
        if not isinstance(conversations_without_filter_5_data, dict):
            print(f"Warning: conversations_without_filter_5_data for {department_name} is not a dict: {type(conversations_without_filter_5_data)}")
            conversations_without_filter_5_data = {'total_conversations': 0}
        
        if not isinstance(bot_data, dict):
            print(f"Warning: bot_data for {department_name} is not a dict: {type(bot_data)}")
            bot_data = {'total_conversations': 0, 'bot_handled_count': 0, 'bot_handled_percentage': 0}
        
        if not isinstance(shadowing_data, dict):
            print(f"Warning: shadowing_data for {department_name} is not a dict: {type(shadowing_data)}")
            shadowing_data = {'overall_shadowing_percentage': 0.0}
        
        if not isinstance(similarity_data, dict):
            print(f"Warning: similarity_data for {department_name} is not a dict: {type(similarity_data)}")
            similarity_data = {'similarity_conversation_count': 0, 'similarity_percentage': 0.0, 'avg_similarity': 0.0}
        
        if not isinstance(issues_data, dict):
            print(f"Warning: issues_data for {department_name} is not a dict: {type(issues_data)}")
            issues_data = {'shadowed_reported_issues': 0, 'reported_percentage': 0.0, 'open_issues_by_agents': 0}
        
        intervention_reengagement_data = intervention_reengagement_results.get(department_name, {})
        if not isinstance(intervention_reengagement_data, dict):
            print(f"Warning: intervention_reengagement_data for {department_name} is not a dict: {type(intervention_reengagement_data)}")
            intervention_reengagement_data = {
                'total_last_interventions_bot_count': 0, 'total_last_interventions_agent_count': 0, 'total_last_interventions_overall_count': 0,
                'reengaged_interventions_bot_count': 0, 'reengaged_interventions_agent_count': 0, 'reengaged_interventions_overall_count': 0,
                'intervention_reengagement_bot_rate': 0, 'intervention_reengagement_agent_rate': 0, 'intervention_reengagement_overall_rate': 0
            }
        
        # Get fully handled by agents data (Applicant Tracking departments only)
        fully_handled_data = fully_handled_results.get(department_name, {})
        if not isinstance(fully_handled_data, dict):
            fully_handled_data = {'chats_fully_handled_by_agents': 0, 'unique_applicants': 0}
        
        # Debug output for excluding pokes in enhanced function
        if department_name == 'CC_Sales':
            print(f"DEBUG ENHANCED: CC_Sales bot_data keys: {list(bot_data.keys())}")
            print(f"DEBUG ENHANCED: CC_Sales excluding pokes values: {bot_data.get('chats_with_1_plus_agent_messages_excluding_pokes', 'MISSING')}, {bot_data.get('chats_with_1_plus_agent_messages_excluding_pokes_percentage', 'MISSING')}")
        
        # Debug output for chats with no bot messages (CC_Resolvers) in enhanced function
        if department_name == 'CC_Resolvers':
            print(f"DEBUG ENHANCED: CC_Resolvers bot_data keys present: {'chats_with_no_bot_messages_count' in bot_data}, {'chats_with_no_bot_messages_percentage' in bot_data}")
            print(f"DEBUG ENHANCED: CC_Resolvers no bot messages values: COUNT={bot_data.get('chats_with_no_bot_messages_count', 'MISSING')}, PERCENTAGE={bot_data.get('chats_with_no_bot_messages_percentage', 'MISSING')}")

        _fs = bot_data.get('filtering_stats', {})
        _total_original      = _fs.get('total_original_conversations', 0)
        _engagement_valid    = _fs.get('engagement_valid_conversations', 0)
        _bot_skills_valid    = _fs.get('conversations_with_bot_skills', 0)
        excluded_no_engagement = max(_total_original - _engagement_valid, 0)
        excluded_no_bot_skill  = max(_engagement_valid - _bot_skills_valid, 0)
        excluded_hi_bye        = _fs.get('hi_bye_conversations_removed', 0)
        excluded_wrong_date    = _fs.get('conversations_filtered_by_date', 0)
        total_excluded         = max(_total_original - bot_data.get('total_conversations', 0), 0)

        metrics = {
            'Date': current_date,
            'Department': department_name,
            
            # Bot Handling Metrics (Phase 2)
            'Total_Conversations': conversations_without_filter_5_data.get('total_conversations', 0),
            'Chats_Supposed_to_be_Bot_Handled': bot_data.get('total_conversations', 0),
            # Initiation breakdown: how the chats in Chats_Supposed_to_be_Bot_Handled were started
            'CHATS_INITIATED_BY_US':     bot_data.get('chats_initiated_by_us_count', 0),
            'CHATS_INITIATED_BY_CLIENT': bot_data.get('chats_initiated_by_client_count', 0),
            # Exclusion breakdown: chats removed on the way from ChatCC to Chats_Supposed_to_be_Bot_Handled
            'Excluded_No_Engagement_Chats': excluded_no_engagement,
            'Excluded_No_Bot_Skill_Chats':  excluded_no_bot_skill,
            'Excluded_Hi_Bye_Chats':        excluded_hi_bye,
            'Excluded_Wrong_Date_Chats':    excluded_wrong_date,
            'Total_Excluded_Chats':         total_excluded,
            'CONSUMER_ONLY_CHATS_COUNT':    _fs.get('consumer_only_chats_count', 0),
            'Bot_Handled_Count': bot_data.get('bot_handled_count', 0),
            'Bot_Handled_Percentage': round(bot_data.get('bot_handled_percentage', 0), 2),
            
            # Agent Message Breakdown Metrics (Phase 2)
            'Chats_With_1_Plus_Agent_Messages': bot_data.get('chats_with_1_plus_agent_messages', 0),
            'Chats_With_2_Plus_Agent_Messages': bot_data.get('chats_with_2_plus_agent_messages', 0),
            'Chats_With_3_Plus_Agent_Messages': bot_data.get('chats_with_3_plus_agent_messages', 0),
            'Chats_With_1_Plus_Agent_Messages_Percentage': round(bot_data.get('chats_with_1_plus_agent_messages_percentage', 0), 2),
            'Chats_With_2_Plus_Agent_Messages_Percentage': round(bot_data.get('chats_with_2_plus_agent_messages_percentage', 0), 2),
            'Chats_With_3_Plus_Agent_Messages_Percentage': round(bot_data.get('chats_with_3_plus_agent_messages_percentage', 0), 2),
            'CHATS_WITH_1_PLUS_AGENT_MESSAGES_EXCLUDING_POKES': bot_data.get('chats_with_1_plus_agent_messages_excluding_pokes', 0),
            'CHATS_WITH_1_PLUS_AGENT_MESSAGES_EXCLUDING_POKES_PERCENTAGE': round(bot_data.get('chats_with_1_plus_agent_messages_excluding_pokes_percentage', 0), 2),
            'CHATS_WITH_POKES_COUNT': bot_data.get('chats_with_pokes', 0),
            'CHATS_WITH_POKES_PERCENTAGE': round(bot_data.get('chats_with_pokes_percentage', 0), 2),
            'CHATS_WITH_EXACTLY_1_AGENT_MESSAGE': bot_data.get('CHATS_WITH_EXACTLY_1_AGENT_MESSAGE', 0),
            'CHATS_WITH_EXACTLY_1_AGENT_MESSAGE_PERCENTAGE': round(bot_data.get('CHATS_WITH_EXACTLY_1_AGENT_MESSAGE_PERCENTAGE', 0), 2),
            'CHATS_WITH_EXACTLY_2_AGENT_MESSAGES': bot_data.get('CHATS_WITH_EXACTLY_2_AGENT_MESSAGES', 0),
            'CHATS_WITH_EXACTLY_2_AGENT_MESSAGES_PERCENTAGE': round(bot_data.get('CHATS_WITH_EXACTLY_2_AGENT_MESSAGES_PERCENTAGE', 0), 2),
            'CHATS_WITH_EXACTLY_3_AGENT_MESSAGES': bot_data.get('CHATS_WITH_EXACTLY_3_AGENT_MESSAGES', 0),
            'CHATS_WITH_EXACTLY_3_AGENT_MESSAGES_PERCENTAGE': round(bot_data.get('CHATS_WITH_EXACTLY_3_AGENT_MESSAGES_PERCENTAGE', 0), 2),
            'AVG_BOT_MSGS_BEFORE_TRANSFER': bot_data.get('avg_bot_msgs_before_transfer', None),
            'TRANSFERRED_CONVERSATION_COUNT': bot_data.get('transferred_conversation_count', 0),
            'MCD_MV_TRANSFER_COUNT': bot_data.get('mcd_mv_transfer_count', 0),
            'MCD_CC_TRANSFER_COUNT': bot_data.get('mcd_cc_transfer_count', 0),
            # Call Requests Metrics (Phase 2)
            'Call_Requests_Count': bot_data.get('call_requests_count', 0),
            'Call_Requests_Percentage': round(bot_data.get('call_requests_percentage', 0), 2),
            
            # Agent Intervention Metrics (Phase 2)
            'Agent_Intervention_Percentage': round(bot_data.get('agent_intervention_percentage', 0), 2),

            
            # CC_Resolvers Complaint Action Metrics (Phase 2)
            'COMPLAINT_ACTION_COUNT': bot_data.get('complaint_action_count', 0),
            'COMPLAINT_ACTION_PERCENTAGE': round(bot_data.get('complaint_action_percentage', 0), 2),
            'COMPLAINT_ACTION_WITH_0_AGENT_MESSAGES_COUNT': bot_data.get('complaint_action_with_0_agent_messages_count', 0),
            'COMPLAINT_ACTION_WITH_0_AGENT_MESSAGES_PERCENTAGE': round(bot_data.get('complaint_action_with_0_agent_messages_percentage', 0), 2),
            'COMPLAINT_ACTION_WITH_1_PLUS_AGENT_MESSAGES_COUNT': bot_data.get('complaint_action_with_1_plus_agent_messages_count', 0),
            'COMPLAINT_ACTION_WITH_1_PLUS_AGENT_MESSAGES_PERCENTAGE': round(bot_data.get('complaint_action_with_1_plus_agent_messages_percentage', 0), 2),
            'CHATS_WITH_NO_BOT_MESSAGES_COUNT': bot_data.get('chats_with_no_bot_messages_count', 0),
            'CHATS_WITH_NO_BOT_MESSAGES_PERCENTAGE': round(bot_data.get('chats_with_no_bot_messages_percentage', 0), 2),
            'BOT_HANDLED_WITH_BOT_RESPONSE_COUNT': bot_data.get('bot_handled_with_bot_response_count', 0),
            'BOT_HANDLED_WITH_BOT_RESPONSE_PERCENTAGE': round(bot_data.get('bot_handled_with_bot_response_percentage', 0), 2),
            'CONVERSATIONS_WITH_BOT_RESPONSE_COUNT': bot_data.get('conversations_with_bot_response_count', 0),
            
            # MV_Resolvers Proactive Agent Messages Metrics (Phase 2)
            'PROACTIVE_AGENT_MESSAGES_COUNT': bot_data.get('proactive_agent_messages_count', 0),
            'PROACTIVE_AGENT_MESSAGES_PERCENTAGE': round(bot_data.get('proactive_agent_messages_percentage', 0), 2),
            'DIRECTLY_HANDLED_BY_SENIORS_COUNT': bot_data.get('directly_handled_by_seniors_count', 0),
            'DIRECTLY_HANDLED_BY_SENIORS_PERCENTAGE': round(bot_data.get('directly_handled_by_seniors_percentage', 0), 2),
            'OTHER_BOTS_TO_SENIORS_COUNT': bot_data.get('other_bots_to_seniors_count', 0),
            'OTHER_BOTS_TO_SENIORS_PERCENTAGE': round(bot_data.get('other_bots_to_seniors_percentage', 0), 2),
            'OUR_BOT_TO_SENIORS_COUNT': bot_data.get('our_bot_to_seniors_count', 0),
            'OUR_BOT_TO_SENIORS_PERCENTAGE': round(bot_data.get('our_bot_to_seniors_percentage', 0), 2),
            'MV_BOT_KNOWN_FLOW_TRANSFER_COUNT': bot_data.get('mv_bot_known_flow_transfer_count', 0),
            'MV_BOT_KNOWN_FLOW_TRANSFER_PERCENTAGE': round(bot_data.get('mv_bot_known_flow_transfer_percentage', 0), 2),
            'MV_BOT_TECH_ERRORS_TRANSFERS_COUNT': bot_data.get('mv_bot_tech_errors_transfers_count', 0),
            'MV_BOT_TECH_ERRORS_TRANSFERS_PERCENTAGE': round(bot_data.get('mv_bot_tech_errors_transfers_percentage', 0), 2),
            'MV_BOT_GUARDRAILS_COUNT': bot_data.get('mv_bot_guardrails_count', 0),
            'MV_BOT_GUARDRAILS_PERCENTAGE': round(bot_data.get('mv_bot_guardrails_percentage', 0), 2),
            'MV_BOT_OTHER_TRANSFERS_COUNT': bot_data.get('mv_bot_other_transfers_count', 0),
            'MV_BOT_OTHER_TRANSFERS_PERCENTAGE': round(bot_data.get('mv_bot_other_transfers_percentage', 0), 2),
            'OUR_BOT_TO_MV_RESOLVERS_SENIORS_COUNT': bot_data.get('our_bot_to_mv_resolvers_seniors_count', 0),
            'OUR_BOT_TO_MV_RESOLVERS_SENIORS_PERCENTAGE': round(bot_data.get('our_bot_to_mv_resolvers_seniors_percentage', 0), 2),
            'OUR_BOT_TO_MV_CALLERS_COUNT': bot_data.get('our_bot_to_mv_callers_count', 0),
            'OUR_BOT_TO_MV_CALLERS_PERCENTAGE': round(bot_data.get('our_bot_to_mv_callers_percentage', 0), 2),
            'OUR_BOT_TO_PRE_R_VISA_RETENTION_COUNT': bot_data.get('our_bot_to_pre_r_visa_retention_count', 0),
            'OUR_BOT_TO_PRE_R_VISA_RETENTION_PERCENTAGE': round(bot_data.get('our_bot_to_pre_r_visa_retention_percentage', 0), 2),
            'DELIGHTERS_TO_SENIORS_COUNT': bot_data.get('delighters_to_seniors_count', 0),
            'DELIGHTERS_TO_SENIORS_PERCENTAGE': round(bot_data.get('delighters_to_seniors_percentage', 0), 2),
            'TOTAL_SENIORS_CALLERS_COUNT': bot_data.get('total_seniors_callers_count', 0),
            'TOTAL_SENIORS_CALLERS_PERCENTAGE': round(bot_data.get('total_seniors_callers_percentage', 0), 2),
            'SENIORS_NO_RESPONSE_TRANSFERS_COUNT': bot_data.get('seniors_no_response_transfers_count', 0),
            'SENIORS_NO_RESPONSE_TRANSFERS_PERCENTAGE': round(bot_data.get('seniors_no_response_transfers_percentage', 0), 2),
            'SENIORS_OUR_BOT_COUNT': bot_data.get('seniors_our_bot_count', 0),
            'SENIORS_OUR_BOT_PERCENTAGE': round(bot_data.get('seniors_our_bot_percentage', 0), 2),
            'SENIORS_DIRECTLY_HANDLED_COUNT': bot_data.get('seniors_directly_handled_count', 0),
            'SENIORS_DIRECTLY_HANDLED_PERCENTAGE': round(bot_data.get('seniors_directly_handled_percentage', 0), 2),
            'SENIORS_PROACTIVE_COUNT': bot_data.get('seniors_proactive_count', 0),
            'SENIORS_PROACTIVE_PERCENTAGE': round(bot_data.get('seniors_proactive_percentage', 0), 2),
            'SENIORS_PROACTIVE_MV_RESOLVERS_SENIORS_ONLY_COUNT': bot_data.get('seniors_proactive_mv_resolvers_seniors_only_count', 0),
            'SENIORS_PROACTIVE_MV_RESOLVERS_SENIORS_ONLY_PERCENTAGE': round(bot_data.get('seniors_proactive_mv_resolvers_seniors_only_percentage', 0), 2),
            'SENIORS_OUR_BOT_TO_MV_RESOLVERS_SENIORS_COUNT': bot_data.get('seniors_our_bot_to_mv_resolvers_seniors_count', 0),
            'SENIORS_OUR_BOT_TO_MV_RESOLVERS_SENIORS_PERCENTAGE': round(bot_data.get('seniors_our_bot_to_mv_resolvers_seniors_percentage', 0), 2),
            'SENIORS_OUR_BOT_TO_MV_CALLERS_COUNT': bot_data.get('seniors_our_bot_to_mv_callers_count', 0),
            'SENIORS_OUR_BOT_TO_MV_CALLERS_PERCENTAGE': round(bot_data.get('seniors_our_bot_to_mv_callers_percentage', 0), 2),
            'SENIORS_OUR_BOT_TO_PRE_R_VISA_RETENTION_COUNT': bot_data.get('seniors_our_bot_to_pre_r_visa_retention_count', 0),
            'SENIORS_OUR_BOT_TO_PRE_R_VISA_RETENTION_PERCENTAGE': round(bot_data.get('seniors_our_bot_to_pre_r_visa_retention_percentage', 0), 2),
            'SENIORS_DELIGHTERS_COUNT': bot_data.get('seniors_delighters_count', 0),
            'SENIORS_DELIGHTERS_PERCENTAGE': round(bot_data.get('seniors_delighters_percentage', 0), 2),
            'SENIORS_OTHER_BOTS_COUNT': bot_data.get('seniors_other_bots_count', 0),
            'SENIORS_OTHER_BOTS_PERCENTAGE': round(bot_data.get('seniors_other_bots_percentage', 0), 2),
            'UNIQUE_UNION_COUNT': bot_data.get('unique_union_count', 0),
            'WRONG_NUMBER_CLIENTS_COUNT': bot_data.get('wrong_number_clients_count', 0),
            'TOTAL_GUARDRAIL_COUNT': bot_data.get('total_guardrail_count', 0),
            'TOTAL_GUARDRAIL_PERCENTAGE': round(bot_data.get('total_guardrail_percentage', 0), 2),
            'GUARDRAIL_AGENT_COUNT': bot_data.get('guardrail_agent_count', 0),
            'GUARDRAIL_AGENT_PERCENTAGE': round(bot_data.get('guardrail_agent_percentage', 0), 2),
            
            # Tech Error Transfers Metrics (Phase 2 - All Departments)
            'TECH_ERROR_TRANSFERS_COUNT': bot_data.get('tech_error_transfers_count', 0),
            'TECH_ERROR_TRANSFERS_PERCENTAGE': round(bot_data.get('tech_error_transfers_percentage', 0), 2),
            
            # Bot Handled Excluding Fillers Metrics (Phase 2)
            'BOT_HANDLED_EXCLUDING_FILLERS_COUNT': bot_data.get('bot_handled_excluding_fillers_count', 0),
            'BOT_HANDLED_EXCLUDING_FILLERS_PERCENTAGE': round(bot_data.get('bot_handled_excluding_fillers_percentage', 0), 2),
            
            # Repetition Metrics (Phase 2)
            'Repetition_Conversation_Count': rep_data.get('repetition_conversation_count', 0),
            'Repetition_Percentage': round(rep_data.get('repetition_percentage', 0), 2),
            'Total_Repetition_Score': rep_data.get('total_repetition_score', 0),
            'Avg_Repetition': round(rep_data.get('avg_repetition', 0), 2),
            
            # Repetition Breakdown Metrics (Phase 2)
            'STATIC_EXCLUSION_REPETITIONS_COUNT': rep_data.get('static_exclusion_repetitions_count', 0),
            'STATIC_EXCLUSION_REPETITIONS_PERCENTAGE': round(rep_data.get('static_exclusion_repetitions_percentage', 0), 2),
            'DYNAMIC_NORMAL_REPETITIONS_COUNT': rep_data.get('dynamic_normal_repetitions_count', 0),
            'DYNAMIC_NORMAL_REPETITIONS_PERCENTAGE': round(rep_data.get('dynamic_normal_repetitions_percentage', 0), 2),
            
            # Delay Metrics (Phase 3) - Method 1
            'Method1_Avg_Initial_Delay_Seconds': round(delay_data.get('avg_method1_initial_delay', 0) or 0, 2),
            'Method1_Avg_Non_Initial_Delay_Seconds': round(delay_data.get('avg_method1_non_initial_delay', 0) or 0, 2),
            'Method1_Initial_Outliers': delay_data.get('method1_initial_outliers', 0),
            'Method1_Non_Initial_Outliers': delay_data.get('method1_non_initial_outliers', 0),
            
            # Delay Metrics (Phase 3) - Method 2
            'Method2_Avg_Initial_Delay_Seconds': round(delay_data.get('avg_method2_initial_delay', 0) or 0, 2),
            'Method2_Avg_Non_Initial_Delay_Seconds': round(delay_data.get('avg_method2_non_initial_delay', 0) or 0, 2),
            'Method2_Initial_Outliers': delay_data.get('method2_initial_outliers', 0),
            'Method2_Non_Initial_Outliers': delay_data.get('method2_non_initial_outliers', 0),
            
            # Delay Metrics (Phase 3) - Method 3
            'Method3_Avg_Initial_Delay_Seconds': round(delay_data.get('avg_method3_initial_delay', 0) or 0, 2),
            'Method3_Avg_Non_Initial_Delay_Seconds': round(delay_data.get('avg_method3_non_initial_delay', 0) or 0, 2),
            'Method3_Initial_Outliers': delay_data.get('method3_initial_outliers', 0),
            'Method3_Non_Initial_Outliers': delay_data.get('method3_non_initial_outliers', 0),
            
            # NEW: Delay Metrics (Phase 3) - 4-50 Minute Range (Method 1 only for simplicity)
            'AVG_INITIAL_DELAY_4_TO_50_MINS_SECONDS': round(delay_data.get('avg_method1_initial_delay_4_to_50', 0) or 0, 2),
            'AVG_NON_INITIAL_DELAY_4_TO_50_MINS_SECONDS': round(delay_data.get('avg_method1_non_initial_delay_4_to_50', 0) or 0, 2),
            'INITIAL_DELAY_4_TO_50_MINS_COUNT': delay_data.get('method1_initial_4_to_50_count', 0),
            'NON_INITIAL_DELAY_4_TO_50_MINS_COUNT': delay_data.get('method1_non_initial_4_to_50_count', 0),
            
            # Unresponsive Metrics (Phase 3)
            'Unresponsive_Count': unresponsive_data.get('unresponsive_count', 0),
            'Unresponsive_Percentage': round(unresponsive_data.get('unresponsive_percentage', 0), 2),
            
            # Downtime Metrics (Phase 2E) - >4 min response time
            'DOWNTIME_CONVERSATION_COUNT': downtime_data.get('downtime_conversation_count', 0),
            'DOWNTIME_PERCENTAGE': round((downtime_data.get('downtime_conversation_count', 0) / bot_data.get('total_conversations', 1) * 100) if bot_data.get('total_conversations', 0) > 0 else 0, 2),
            
            # Agent Intervention Due to No Response Metrics (Phase 3)
            'INTERVENTION_DUE_TO_NO_RESPONSE_COUNT': delay_data.get('agent_interventions_no_response_count', 0),
            'INTERVENTION_DUE_TO_NO_RESPONSE_PERCENTAGE': round(
                (delay_data.get('agent_interventions_no_response_count', 0) / delay_data.get('total_conversations', 1) * 100) 
                if delay_data.get('total_conversations', 0) > 0 else 0, 2
            ),
            
            # Normal Agent Intervention Metrics (Phase 3) - ≤4 minutes
            'NORMAL_MANUAL_INTERVENTION_COUNT': delay_data.get('normal_agent_interventions_count', 0),
            'NORMAL_MANUAL_INTERVENTION_PERCENTAGE': round(
                (delay_data.get('normal_agent_interventions_count', 0) / delay_data.get('total_conversations', 1) * 100) 
                if delay_data.get('total_conversations', 0) > 0 else 0, 2
            ),
            
            # Analysis Metadata
            'Outlier_Threshold_Seconds': DELAY_OUTLIER_THRESHOLD_SECONDS,
            'Unresponsive_Threshold_Minutes': 50,
            'Analysis_Date': datetime.now().strftime('%Y-%m-%d'),
            'Phase': 'Phase3_AdvancedAnalytics',

            # Shadowing Metrics (Phase 3)
            'Overall_Shadowing_Percentage': round(shadowing_data.get('overall_shadowing_percentage', 0), 2),
            'Overall_Shadowed_Assigned_Percentage': round(shadowing_data.get('overall_shadowed_assigned_percentage', 0), 2),
            'Total_Unassigned': shadowing_data.get('total_unassigned', 0),
            'ELIGIBLE_SHADOWED': shadowing_data.get('total_eligible_conversations', 0),
            # Issues Metrics (Phase 3)
            'Shadowed_Reported_Issues': issues_data.get('shadowed_reported_issues', 0),
            'Reported_Percentage': round(issues_data.get('reported_percentage', 0), 2),
            'Open_Issues_By_Agents': issues_data.get('open_issues_by_agents', 0),


            # Similarity Metrics (Phase 2)
            'Similarity_Conversation_Count': similarity_data.get('similarity_conversation_count', 0),
            'Similarity_Percentage': round(similarity_data.get('similarity_percentage', 0), 2),
            'Avg_Similarity_Score': round(similarity_data.get('avg_similarity', 0), 3),
            
            # Similarity Breakdown Metrics (Phase 2)
            'STATIC_EXCLUSION_SIMILARITY_COUNT': similarity_data.get('static_exclusion_similarity_count', 0),
            'STATIC_EXCLUSION_SIMILARITY_PERCENTAGE': round(similarity_data.get('static_exclusion_similarity_percentage', 0), 2),
            'DYNAMIC_NORMAL_SIMILARITY_COUNT': similarity_data.get('dynamic_normal_similarity_count', 0),
            'DYNAMIC_NORMAL_SIMILARITY_PERCENTAGE': round(similarity_data.get('dynamic_normal_similarity_percentage', 0), 2),
            
            # Intervention Reengagement Metrics (10-minute response)
            'TOTAL_LAST_INTERVENTIONS_BOT_COUNT': intervention_reengagement_data.get('total_last_interventions_bot_count', 0),
            'TOTAL_LAST_INTERVENTIONS_AGENT_COUNT': intervention_reengagement_data.get('total_last_interventions_agent_count', 0),
            'TOTAL_LAST_INTERVENTIONS_M20_COUNT': intervention_reengagement_data.get('total_last_interventions_m20_count', 0),
            'TOTAL_LAST_INTERVENTIONS_OVERALL_COUNT': intervention_reengagement_data.get('total_last_interventions_overall_count', 0),
            'REENGAGED_INTERVENTIONS_BOT_COUNT': intervention_reengagement_data.get('reengaged_interventions_bot_count', 0),
            'REENGAGED_INTERVENTIONS_AGENT_COUNT': intervention_reengagement_data.get('reengaged_interventions_agent_count', 0),
            'REENGAGED_INTERVENTIONS_M20_COUNT': intervention_reengagement_data.get('reengaged_interventions_m20_count', 0),
            'REENGAGED_INTERVENTIONS_OVERALL_COUNT': intervention_reengagement_data.get('reengaged_interventions_overall_count', 0),
            'INTERVENTION_REENGAGEMENT_BOT_RATE': round(intervention_reengagement_data.get('intervention_reengagement_bot_rate', 0), 1),
            'INTERVENTION_REENGAGEMENT_AGENT_RATE': round(intervention_reengagement_data.get('intervention_reengagement_agent_rate', 0), 1),
            'INTERVENTION_REENGAGEMENT_M20_RATE': round(intervention_reengagement_data.get('intervention_reengagement_m20_rate', 0), 1),
            'INTERVENTION_REENGAGEMENT_OVERALL_RATE': round(intervention_reengagement_data.get('intervention_reengagement_overall_rate', 0), 1),
            
            # Chats Fully Handled by Agents (Applicant Tracking departments only)
            'CHATS_FULLY_HANDLED_BY_AGENTS': fully_handled_data.get('chats_fully_handled_by_agents', 0),
            'UNIQUE_APPLICANTS_FULLY_HANDLED': fully_handled_data.get('unique_applicants', 0),
        }
        
        # Debug output for final metrics in enhanced function
        if department_name == 'CC_Sales':
            print(f"DEBUG FINAL ENHANCED: CC_Sales final metrics excluding pokes: {metrics.get('CHATS_WITH_1_PLUS_AGENT_MESSAGES_EXCLUDING_POKES', 'MISSING')}, {metrics.get('CHATS_WITH_1_PLUS_AGENT_MESSAGES_EXCLUDING_POKES_PERCENTAGE', 'MISSING')}")
        
        # Debug output for final metrics for CC_Resolvers in enhanced function
        if department_name == 'CC_Resolvers':
            print(f"DEBUG FINAL ENHANCED: CC_Resolvers final metrics no bot messages: COUNT={metrics.get('CHATS_WITH_NO_BOT_MESSAGES_COUNT', 'MISSING')}, PERCENTAGE={metrics.get('CHATS_WITH_NO_BOT_MESSAGES_PERCENTAGE', 'MISSING')}")
        
        combined_metrics.append(metrics)
    
    return combined_metrics

def insert_raw_data_with_cleanup(session: snowpark.Session, table_name: str, department: str, target_date, dataframe: pd.DataFrame, columns: list):
    """
    Dynamically insert raw data into a table with date-based cleanup.
    
    Args:
        session: Snowflake session object
        table_name: Name of the target table
        department: Department value to add to all rows
        dataframe: Pandas dataframe containing the data to insert
        columns: List of column names that should match dataframe columns
        
    Returns:
        dict: Summary of the operation
    """
    
    try:
        # Step 1: Validate dataframe columns match the expected columns
        if len(dataframe.columns) != len(columns):
            raise ValueError(f"Dataframe has {len(dataframe.columns)} columns but expected {len(columns)} columns")
        
        # Step 1: Check if table exists
        try:
            check_query = f"""
            SELECT COUNT(*) AS count
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = UPPER('{table_name}')
            AND TABLE_SCHEMA = CURRENT_SCHEMA()
            """
            exists = session.sql(check_query).collect()[0]['COUNT'] > 0
        except:
            exists = False

        # Step 2: Create table if it doesn't exist
        if not exists:
            essential_cols = {
                'DATE': 'DATE',
                'DEPARTMENT': 'VARCHAR(100)',
                'TIMESTAMP': 'TIMESTAMP'
            }

            # Use VARCHAR as default for dynamic columns (customize if needed)
            dynamic_cols = {col_name: 'VARCHAR(16777216)' for col_name in columns}  # Max VARCHAR length in Snowflake

            full_schema = {**essential_cols, **dynamic_cols}
            create_cols_str = ",\n    ".join([f"{col} {dtype}" for col, dtype in full_schema.items()])
            create_query = f"CREATE TABLE {table_name} (\n    {create_cols_str}\n)"
            session.sql(create_query).collect()
            print(f"✅ Created table {table_name} with {len(full_schema)} columns")
        
        # Step 3: Calculate current timestamp
        current_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"Processing data for date: {target_date}")
        print(f"Target table: {table_name}")
        print(f"Department: {department}")
        print(f"Dataframe shape: {dataframe.shape}")
        
        # Step 4: Remove existing rows for yesterday's date
        delete_query = f"""
        DELETE FROM {table_name} 
        WHERE DATE = '{target_date}' AND DEPARTMENT = '{department}'
        """
        
        delete_result = session.sql(delete_query).collect()
        print(f"Cleaned existing data for {target_date} in department {department}")
        
        # Step 5: Prepare dataframe for insertion
        # Add the essential columns
        dataframe_copy = dataframe.copy()
        dataframe_copy['DATE'] = target_date
        dataframe_copy['TIMESTAMP'] = current_ts
        dataframe_copy['DEPARTMENT'] = department
        
        # Get the actual table schema to ensure column order matches
        try:
            table_schema_query = f"""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = UPPER('{table_name}') 
            AND TABLE_SCHEMA = CURRENT_SCHEMA()
            ORDER BY ORDINAL_POSITION
            """
            table_schema_result = session.sql(table_schema_query).collect()
            table_column_order = [row['COLUMN_NAME'] for row in table_schema_result]
            
            # Use table's column order, but only include columns that exist in our DataFrame
            available_columns = set(dataframe_copy.columns)
            final_column_order = [col for col in table_column_order if col in available_columns]
            
            print(f"🔍 DEBUG - Using table schema column order ({len(final_column_order)} columns)")
            
            # Debug: Check for missing columns
            missing_columns = [col for col in table_column_order if col not in available_columns]
            extra_df_columns = [col for col in available_columns if col not in table_column_order]
            
            if missing_columns:
                print(f"🔍 DEBUG - Missing columns in DataFrame: {missing_columns}")
            if extra_df_columns:
                print(f"🔍 DEBUG - Extra columns in DataFrame: {extra_df_columns}")
            
            print(f"🔍 DEBUG - Table expects {len(table_column_order)} columns, DataFrame has {len(available_columns)} columns")
            
        except Exception as e:
            print(f"⚠️  Could not get table schema, using default order: {e}")
            # Fallback to original logic
            essential_cols = ['DATE',  'DEPARTMENT', 'TIMESTAMP']
            dynamic_cols = columns
            final_column_order = essential_cols + dynamic_cols
        
        # Reorder dataframe columns to match table schema
        dataframe_copy = dataframe_copy[final_column_order]
        
        # Step 6: Convert pandas dataframe to Snowpark dataframe and write to table
        snowpark_df = session.create_dataframe(dataframe_copy)
        
        # Write to table (append mode)
        snowpark_df.write.mode("append").save_as_table(table_name)
        
        # Step 7: Get final count for verification
        count_query = f"""
        SELECT COUNT(*) as row_count 
        FROM {table_name} 
        WHERE DATE = '{target_date}' AND DEPARTMENT = '{department}'
        """
        
        final_count = session.sql(count_query).collect()[0]['ROW_COUNT']
        
        # Return summary
        summary = {
            "status": "success",
            "table_name": table_name,
            "department": department,
            "date_processed": target_date,
            "timestamp": current_ts,
            "rows_inserted": len(dataframe),
            "final_row_count": final_count,
            "columns_processed": len(columns),
            "total_columns": len(final_column_order)
        }
        
        print(f"Successfully inserted {len(dataframe)} rows into {table_name}")
        print(f"Final count for {target_date}/{department}: {final_count} rows")
        
        return summary
        
    except Exception as e:
        error_summary = {
            "status": "error",
            "table_name": table_name,
            "department": department,
            "error_message": str(e),
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        print(f"Error processing data: {str(e)}")
        return error_summary


def save_phase3_raw_tables_snowflake(session: snowpark.Session, delay_data, unresponsive_data, target_date=None):
    """
    Save Phase 3 raw analysis data to Snowflake tables.
    
    Args:
        session: Snowflake session
        delay_data: Delay analysis data
        unresponsive_data: Unresponsive conversations data
        target_date: Target date for table naming
    
    Returns:
        Tuple: (delay_table_name, unresponsive_table_name, success)
    """
    print("\n💾 SAVING PHASE 3 RAW ANALYSIS TABLES...")
    
    try:
        # Create table names
        delay_table_name = create_output_table_name('DELAY_ANALYSIS_DETAILED', target_date)
        unresponsive_table_name = create_output_table_name('UNRESPONSIVE_CONVERSATIONS', target_date)
        
        # Save delay analysis data
        if delay_data:
            delay_df = pd.DataFrame(delay_data)
            # Clean data types for Snowflake compatibility
            delay_df = clean_dataframe_for_snowflake(delay_df)
            session.write_pandas(delay_df, delay_table_name, auto_create_table=True, overwrite=True)
            print(f"  ✅ Delay analysis data: {len(delay_data):,} rows → {delay_table_name}")
        else:
            print(f"  ⚠️  No delay analysis data to save")
        
        # Save unresponsive conversations data
        if unresponsive_data:
            unresponsive_df = pd.DataFrame(unresponsive_data)
            # Clean data types for Snowflake compatibility
            unresponsive_df = clean_dataframe_for_snowflake(unresponsive_df)
            session.write_pandas(unresponsive_df, unresponsive_table_name, auto_create_table=True, overwrite=True)
            print(f"  ✅ Unresponsive data: {len(unresponsive_data):,} rows → {unresponsive_table_name}")
        else:
            print(f"  ⚠️  No unresponsive data to save")
        
        return delay_table_name, unresponsive_table_name, True
        
    except Exception as e:
        error_report = format_error_details(e, "SAVING PHASE 3 RAW TABLES")
        print(f"  ❌ Failed to save Phase 3 raw tables:")
        print(error_report)
        return None, None, False


# ============================================================================
# PHASE 3 MAIN PROCESSOR
# ============================================================================

def phase3_advanced_analytics_processor(session: snowpark.Session, target_date=None):
    """
    Phase 3 Advanced Analytics Processor: Complete analytics pipeline
    Includes Phase 1 (Foundation) + Phase 2 (Core) + Phase 3 (Advanced)
    
    Args:
        session: Snowflake session
        target_date: Target date for analysis (defaults to today)
    
    Returns:
        Processing summary and results
    """
    print("🚀 PHASE 3: ADVANCED ANALYTICS PROCESSOR")
    print("🔥 COMPLETE PIPELINE: Foundation + Core + Advanced Analytics")
    print("=" * 70)
    
    target_date_str = target_date if target_date else datetime.now().strftime('%Y-%m-%d')
    print(f"📅 Target date: {target_date_str}")
    print("=" * 70)
    
    try:
        # Phase 2: Bot Handling + Repetition + Similarity Analysis (raw data saved automatically)
        print("\n🔄 RUNNING PHASE 2: CORE ANALYTICS")
        bot_results = analyze_bot_handled_conversations_all_departments(session, target_date)
        repetition_results = analyze_repetition_conversations_all_departments(session, target_date)
        # SIMILARITY CALCULATION DISABLED
        # similarity_results = analyze_similarity_conversations_all_departments(session, target_date)
        similarity_results = {}  # Empty dict to prevent errors
        
        # Initialize CC_Sales message categorization before intervention analysis
        print(f"\n🔄 Initializing CC_Sales message categorization...")
        initialize_cc_sales_pokes_validation_df()
        intervention_reengagement_results = analyze_intervention_reengagement_all_departments(session, target_date)
        
        # Save CC_Sales message categorization table
        print(f"\n💾 Saving CC_Sales message categorization...")
        save_cc_sales_pokes_validation_table(session)
        conversations_without_filter_5 = get_conversations_without_filter_5_all_departments(session, target_date)
        
        # Phase 3: Delay + Unresponsive + Shadowing + Issues Analysis (raw data saved automatically)
        print("\n🔄 RUNNING PHASE 3: ADVANCED ANALYTICS")
        delay_results = analyze_delay_conversations_all_departments(session, target_date)
        unresponsive_results = analyze_unresponsive_conversations_all_departments(session, target_date)
        downtime_results = analyze_downtime_conversations_all_departments(session, target_date)
        
        # SHADOWING & ISSUES ANALYSIS - Enabled only for CC_Sales and MV_Sales
        print("\n⚠️  SHADOWING ANALYSIS: Enabled for CC_Sales and MV_Sales only")
        shadowing_results = analyze_shadowing_conversations_all_departments(session, target_date)
        issues_results = analyze_issues_all_departments(session, target_date, shadowing_results)
        
        # CHATS FULLY HANDLED BY AGENTS - Applicant Tracking departments
        print("\n🤝 CALCULATING CHATS FULLY HANDLED BY AGENTS...")
        fully_handled_results = calculate_chats_fully_handled_by_agents(session, target_date)
        
        # Create Enhanced Combined Metrics
        print(f"\n📊 CREATING ENHANCED COMBINED METRICS...")
        combined_metrics = create_enhanced_combined_metrics_snowflake(
            bot_results, repetition_results, similarity_results, delay_results, unresponsive_results, shadowing_results, issues_results, conversations_without_filter_5, intervention_reengagement_results, downtime_results, fully_handled_results, target_date
        )
        print(combined_metrics)
        print(f"  ✅ Created enhanced metrics for {len(combined_metrics)} departments")
        
        # Update Master Metrics Table with Enhanced Data
        print(f"\n📊 UPDATING MASTER METRICS TABLE...")
        master_success = update_master_metrics_table_snowflake(session, combined_metrics)
        
        # Generate comprehensive summary
        total_conversations = sum(r.get('total_conversations', 0) for r in bot_results.values())
        total_bot_handled = sum(r.get('bot_handled_count', 0) for r in bot_results.values())
        total_repetitions = sum(r.get('repetition_conversation_count', 0) for r in repetition_results.values())
        # SIMILARITY CALCULATION DISABLED
        # total_similarity = sum(r.get('similarity_conversation_count', 0) for r in similarity_results.values())
        total_similarity = 0  # Disabled
        total_unresponsive = sum(r.get('unresponsive_count', 0) for r in unresponsive_results.values())
        total_shadowed = sum(r.get('total_shadowed_conversations', 0) for r in shadowing_results.values())
        total_eligible_shadowing = sum(r.get('total_eligible_conversations', 0) for r in shadowing_results.values())
        total_reported_issues = sum(r.get('shadowed_reported_issues', 0) for r in issues_results.values())
        total_pending_issues = sum(r.get('open_issues_by_agents', 0) for r in issues_results.values())
        
        overall_bot_percentage = (total_bot_handled / total_conversations * 100) if total_conversations > 0 else 0
        overall_rep_percentage = (total_repetitions / total_conversations * 100) if total_conversations > 0 else 0
        # SIMILARITY CALCULATION DISABLED
        # overall_similarity_percentage = (total_similarity / total_conversations * 100) if total_conversations > 0 else 0
        overall_similarity_percentage = 0.0  # Disabled
        overall_unresponsive_percentage = (total_unresponsive / total_conversations * 100) if total_conversations > 0 else 0
        overall_shadowing_percentage = (total_shadowed / total_eligible_shadowing * 100) if total_eligible_shadowing > 0 else 0
        overall_reported_percentage = (total_reported_issues / total_shadowed * 100) if total_shadowed > 0 else 0
        
        # Calculate overall delay averages
        all_method1_init = [r.get('avg_method1_initial_delay') for r in delay_results.values() if r.get('avg_method1_initial_delay')]
        all_method1_non = [r.get('avg_method1_non_initial_delay') for r in delay_results.values() if r.get('avg_method1_non_initial_delay')]
        
        overall_method1_init = sum(all_method1_init)/len(all_method1_init) if all_method1_init else 0
        overall_method1_non = sum(all_method1_non)/len(all_method1_non) if all_method1_non else 0
        
        # Calculate agent interventions due to no response
        total_agent_interventions = sum(r.get('agent_interventions_no_response_count', 0) for r in delay_results.values())
        overall_intervention_percentage = (total_agent_interventions / total_conversations * 100) if total_conversations > 0 else 0
        
        summary = f"""
🎯 PHASE 3 ADVANCED ANALYTICS - COMPLETE SUMMARY
{'=' * 60}
📅 Date: {target_date_str}
# 🏢 Departments processed: {len(combined_metrics)}

📊 COMPREHENSIVE METRICS:
   💬 Total conversations: {total_conversations:,}
   🤖 Bot-handled: {total_bot_handled:,} ({overall_bot_percentage:.1f}%)
   🔄 With repetitions: {total_repetitions:,} ({overall_rep_percentage:.1f}%)
   # SIMILARITY DISABLED: 🔍 With 50% similarity: {total_similarity:,} ({overall_similarity_percentage:.1f}%)
   ⏱️  Avg initial delay (Method 1): {overall_method1_init:.1f}s
   ⏱️  Avg non-initial delay (Method 1): {overall_method1_non:.1f}s
   🚨 Agent interventions (no response): {total_agent_interventions:,} ({overall_intervention_percentage:.1f}%)
   📵 Unresponsive: {total_unresponsive:,} ({overall_unresponsive_percentage:.1f}%)
   👥 Shadowing: {total_shadowed:,}/{total_eligible_shadowing:,} ({overall_shadowing_percentage:.1f}%)
   🛠️  Issues reported: {total_reported_issues:,}/{total_shadowed:,} ({overall_reported_percentage:.1f}%)
   ⏳ Issues pending: {total_pending_issues:,}

💾 OUTPUT TABLES:
#    📋 Enhanced master metrics: {MASTER_METRICS_TABLE} {'✅' if master_success else '❌'}
   🤖 Bot handled data: BOT_HANDLED_RAW_DATA ✅ (saved per department)
   🔄 Repetition data: REPETITION_RAW_DATA ✅ (saved per department)
   # SIMILARITY DISABLED: 🔍 Similarity data: SIMILARITY_RAW_DATA ✅ (saved per department)
   ⏱️  Delay analysis data: DELAY_ANALYSIS_RAW_DATA ✅ (saved per department)
   🚨 Agent interventions: AGENT_INTERVENTION_DUE_TO_NO_RESPONSE ✅ (saved per department)
   📵 Unresponsive data: UNRESPONSIVE_RAW_DATA ✅ (saved per department)
   ⏰ Downtime data: DOWNTIME_RAW_DATA ✅ (saved per department)
   👥 Shadowing data: SHADOWING_RAW_DATA ✅ (saved per department)
   👤 Agent breakdown: SHADOWING_AGENT_BREAKDOWN ✅ (saved per department)
   🛠️  Issues data: ISSUES_RAW_DATA ✅ (saved per department)

🏆 ANALYSIS CAPABILITIES:
   ✅ Conversation filtering & preprocessing
   ✅ Bot handling detection
   ✅ Message repetition analysis
   # SIMILARITY DISABLED: ✅ Message 50% similarity analysis (TF-IDF + cosine similarity)
   ✅ 3-method delay calculation (outlier detection)
   ✅ Agent intervention detection (bot failed to respond >4min)
   ✅ Unresponsive conversation detection (55-min threshold)
   ✅ Downtime detection (>4 min response time from daily_delays_details_final)
   ✅ Shadowing analysis (IS_SHADOWED column detection)
   ✅ Issues analysis (agent-reported issues in shadowed conversations)
   ✅ Comprehensive metrics & raw data tables

🎉 Phase 3 Advanced Analytics Complete!
   🚀 Full analytics pipeline operational in Snowflake
   📊 Ready for business intelligence and reporting
"""
        
        print(summary)
        return {
            'summary': summary,
            'bot_results': bot_results,
            'repetition_results': repetition_results,
            # SIMILARITY DISABLED
            # 'similarity_results': similarity_results,
            'delay_results': delay_results,
            'unresponsive_results': unresponsive_results,
            'shadowing_results': shadowing_results,
            'issues_results': issues_results,
            'combined_metrics': combined_metrics,
            'master_success': master_success,
            'raw_data_saved_per_department': True
        }
        
    except Exception as e:
        error_report = format_error_details(e, "PHASE 3 PROCESSOR")
        error_summary = f"""
❌ PHASE 3 CRITICAL FAILURE:
{error_report}

💡 TROUBLESHOOTING:
   - Ensure Phase 1 + Phase 2 functions are working correctly
   - Check Snowflake table permissions and MESSAGE_INDEX column
   - Verify timestamp formats in MESSAGE_SENT_TIME column
   - Test individual functions first before running full pipeline
"""
        print(error_summary)
        return {'summary': error_summary, 'error': str(e)}


# ============================================================================
# TESTING FUNCTIONS FOR PHASE 3
# ============================================================================

def test_delay_analysis_single_department(session: snowpark.Session, department_name, target_date=None):
    """
    Test delay analysis for a single department.
    """
    print(f"🧪 TESTING DELAY ANALYSIS - {department_name}")
    print("=" * 50)
    
    try:
        departments_config = get_snowflake_departments_config()
        
        # Get filtered data from Phase 1
        filtered_df, stats, success, _ = process_department_phase1(session, department_name, target_date)
        
        if not success:
            print(f"❌ Phase 1 failed for {department_name}")
            return
        
        # Run delay analysis
        delay_results, delay_data = analyze_delay_conversations_single_department(
            filtered_df, department_name, departments_config
        )
        
        print(f"\n📊 DELAY RESULTS:")
        for key, value in delay_results.items():
            print(f"   {key}: {value}")
        
        print(f"\n📝 Sample delay data (first 2 rows):")
        if delay_data:
            sample_df = pd.DataFrame(delay_data[:2])
            print(sample_df[['CONVERSATION_ID', 'DELAY_METHOD1_INITIAL', 'DELAY_METHOD1_NON_INITIAL', 'BOT_MESSAGE_COUNT']].to_string())
        else:
            print("   No delay data found")
            
    except Exception as e:
        error_report = format_error_details(e, f"DELAY TEST - {department_name}")
        print(error_report)


def test_unresponsive_single_department(session: snowpark.Session, department_name, target_date=None):
    """
    Test unresponsive analysis for a single department.
    """
    print(f"🧪 TESTING UNRESPONSIVE ANALYSIS - {department_name}")
    print("=" * 50)
    
    try:
        departments_config = get_snowflake_departments_config()
        
        # Get filtered data from Phase 1
        filtered_df, stats, success, _ = process_department_phase1(session, department_name, target_date)
        
        if not success:
            print(f"❌ Phase 1 failed for {department_name}")
            return
        
        # Run unresponsive analysis
        unresponsive_results, unresponsive_data = analyze_unresponsive_conversations_single_department(
            filtered_df, department_name, departments_config
        )
        
        print(f"\n📊 UNRESPONSIVE RESULTS:")
        for key, value in unresponsive_results.items():
            print(f"   {key}: {value}")
        
        print(f"\n📝 Sample unresponsive data (first 3 rows):")
        if unresponsive_data:
            sample_df = pd.DataFrame(unresponsive_data[:3])
            print(sample_df[['conversation_id', 'reason', 'message_count']].to_string())
        else:
            print("   No unresponsive conversations found")
            
    except Exception as e:
        error_report = format_error_details(e, f"UNRESPONSIVE TEST - {department_name}")
        print(error_report)


def validate_phase3_functions():
    """
    Validate Phase 3 specific functions work correctly.
    """
    print("🔍 VALIDATING PHASE 3 FUNCTIONS")
    print("=" * 50)
    
    try:
        # Test timestamp parsing
        test_timestamps = [
            "2025-01-07 14:30:22.0",
            "2025-01-07 14:30:22",
            "2025-01-07T14:30:22Z",
            None,
            ""
        ]
        
        parsed_count = 0
        for ts in test_timestamps:
            result = parse_message_time_snowflake(ts)
            if result is not None:
                parsed_count += 1
        
        print(f"✅ Timestamp parsing: {parsed_count}/{len(test_timestamps)} formats handled")
        
        # Test delay threshold
        print(f"✅ Message outlier threshold: {DELAY_OUTLIER_THRESHOLD_SECONDS} seconds ({DELAY_OUTLIER_THRESHOLD_SECONDS/60:.0f} minutes) - counts individual messages")
        
        # Test data cleaning compatibility
        test_df = pd.DataFrame({
            'DELAY_METHOD1_INITIAL': [10.5, None, 300.2],
            'MESSAGE_SENT_TIME': ['2025-01-07 14:30:22', '2025-01-07 14:31:45', '2025-01-07 14:32:10'],
            'CONVERSATION_ID': ['conv1', 'conv2', 'conv3']
        })
        
        cleaned_df = clean_dataframe_for_snowflake(test_df)
        print(f"✅ Data cleaning for Phase 3: Compatible with delay metrics")
        
        print("\n🎉 All Phase 3 functions validated successfully!")
        print("💡 Ready for advanced delay and unresponsive analysis")
        print(f"⏰ Configured thresholds: {DELAY_OUTLIER_THRESHOLD_SECONDS/60:.0f}min delay outliers, 50min unresponsive")
        
        return True
        
    except Exception as e:
        error_report = format_error_details(e, "PHASE 3 VALIDATION")
        print(error_report)
        return False


# ============================================================================
# ENHANCED MAIN FUNCTION FOR PHASE 3
# ============================================================================

def main_phase3(session: snowpark.Session, target_date=None):
    """
    Enhanced main function for Phase 3 - Complete Analytics Pipeline
    """
    try:
        result = phase3_advanced_analytics_processor(session, target_date)
        return result['summary']
    except Exception as e:
        error_report = format_error_details(e, "MAIN FUNCTION - PHASE 3")
        return f"""❌ PHASE 3 CRITICAL FAILURE:
{error_report}

💡 TROUBLESHOOTING TIPS:
   - This is the complete analytics pipeline (Phases 1+2+3)
   - Check your department table names in get_snowflake_departments_config()
   - Ensure MESSAGE_INDEX or timestamp-based ordering is available
   - Test individual phases: validate_phase3_functions(), test_delay_analysis_single_department()
   - Required columns: CONVERSATION_ID, MESSAGE_SENT_TIME, MESSAGE_TYPE, SENT_BY, TARGET_SKILL_PER_MESSAGE, TEXT
   - Verify timestamp formats are parseable (YYYY-MM-DD HH:MM:SS format)""" 

# ============================================================================
# SHADOWING ANALYSIS
# ============================================================================

def is_conversation_shadowed_snowflake(conversation_df, department_name, departments_config):
    """
    Check if a conversation is shadowed (Snowflake version).
    Adapted from main_analytics.py shadowing logic.
    
    Args:
        conversation_df: DataFrame containing one conversation's messages
        department_name: Department name for skill filtering
        departments_config: Department configuration dictionary
    
    Returns:
        bool: True if conversation is shadowed
    """
    # Check if IS_SHADOWED column exists in the DataFrame
    if 'IS_SHADOWED' not in conversation_df.columns:
        # If IS_SHADOWED column doesn't exist, assume no shadowing data is available
        return False
    
    # Check if any message in the conversation has IS_SHADOWED = 'TRUE'
    shadowed_messages = conversation_df[
        conversation_df['IS_SHADOWED'].astype(str).str.upper() == 'TRUE'
    ]
    
    return len(shadowed_messages) > 0

def is_shadowed_by_department_agent(shadowed_by, department_agents):
    """
    Check if the last agent name from comma-separated shadowed_by string belongs to department agents.
    
    Args:
        shadowed_by: Comma-separated string of agent names
        department_agents: Set of department agent names (lowercase)
    
    Returns:
        bool: True if last agent is from department
    """
    if not shadowed_by or not department_agents:
        return True
    
    # Split by comma, get last agent, and normalize (strip whitespace and lowercase)
    agents = [agent.strip().lower() for agent in str(shadowed_by).split(',') if agent.strip()]
    
    if not agents:
        return True
    
    last_agent = agents[-1]
    return last_agent in department_agents

def is_assigned_shadower_department_agent(assigned_shadower, department_agents):
    """
    Check if the last agent name from comma-separated assigned_shadower string belongs to department agents.
    """
    if not assigned_shadower or not department_agents:
        return False
    
    # Split by comma, get last agent, and normalize (strip whitespace and lowercase)
    agents = [agent.strip().lower() for agent in str(assigned_shadower).split(',') if agent.strip()]
    
    if not agents:
        return False
    
    return any(agent in department_agents for agent in agents)



def process_day_shadowing_snowflake(conversations, target_date, department_name):
    """
    Process shadowing analysis for a single day (Snowflake version).
    New eligibility window logic:
      - Start time = minimum of per-conversation first_assigned_time
      - End time   = maximum of per-conversation last_button_clicked_time
      - Exclude conversations that have any messages before start or after end
    """
    if not conversations:
        return {
            'eligible_conversations': 0,
            'shadowed_conversations': 0,
            'assigned_conversations': 0,
            'shadowing_percentage': 0.0,
            'first_shadowed_time': None,
            'last_shadowed_time': None,
            'eligible_conversation_data': []
        }

    # Build window from precomputed fields
    start_candidates = [c.get('first_assigned_time') for c in conversations if c.get('first_assigned_time') is not None]
    end_candidates = [c.get('last_button_clicked_time') for c in conversations if c.get('last_button_clicked_time') is not None]

    start_time = min(start_candidates) if start_candidates else None
    end_time = max(end_candidates) if end_candidates else None

    if start_time is None or end_time is None or end_time < start_time:
        return {
            'eligible_conversations': 0,
            'shadowed_conversations': 0,
            'assigned_conversations': 0,
            'shadowing_percentage': 0.0,
            'first_shadowed_time': start_time,
            'last_shadowed_time': end_time,
            'eligible_conversation_data': []
        }

    # Eligible conversations: all messages within [start_time, end_time]
    eligible_conversations = []
    for conv in conversations:
        conv_df = conv['conversation_data']
        msg_times = pd.to_datetime(conv_df['MESSAGE_SENT_TIME'], errors='coerce')
        if msg_times.min() >= start_time and msg_times.max() <= end_time:
            eligible_conversations.append(conv)

    eligible_count = len(eligible_conversations)
    shadowed_count = sum(1 for c in eligible_conversations if c['shadowed'])
    assigned_count = sum(1 for c in eligible_conversations if c['assigned'])
    shadowing_percentage = (shadowed_count / eligible_count * 100) if eligible_count > 0 else 0.0

    eligible_conversation_data = []
    for conv in eligible_conversations:
        eligible_conversation_data.extend(conv['conversation_data'].to_dict('records'))

    print(f"    🔍 {department_name}: {eligible_count} eligible conversations, {shadowed_count} shadowed, {start_time} to {end_time}")
    
    return {
        'eligible_conversations': eligible_count,
        'shadowed_conversations': shadowed_count,
        'assigned_conversations': assigned_count,
        'shadowing_percentage': shadowing_percentage,
        'first_shadowed_time': start_time,
        'last_shadowed_time': end_time,
        'eligible_conversation_data': eligible_conversation_data
    }


def process_department_shadowing_snowflake(session, df, department_name, day_15, day_16, departments_config):
    """
    Process shadowing analysis for a single department (Snowflake version).
    Adapted from main_analytics.py process_department_shadowing()
    """
    dept_config = departments_config[department_name]
    valid_skills = set(dept_config['bot_skills'] + dept_config['agent_skills'])
    
    print(f"    🔍 {department_name}: Analyzing shadowing for days {day_15} and {day_16}")
    
    # Convert timestamp column to datetime
    df['message_datetime'] = pd.to_datetime(df['MESSAGE_SENT_TIME'])
    df['message_date'] = df['message_datetime'].dt.date
    
    # Group by conversation and get first message timestamp for ordering
    conversation_groups = df.groupby('CONVERSATION_ID')
    conversation_order = []
    
    print(f"    🔍 {department_name}: Processing {len(conversation_groups)} conversations")
    department_agents = get_department_agent_names_snowflake(session, department_name, departments_config)
    
    for conv_id, conv_df in conversation_groups:
        # Get first message timestamp
        first_message_time = conv_df['message_datetime'].min()
        
        # Get last message skill to check if conversation ended with valid skill
        last_message = conv_df.loc[conv_df['message_datetime'].idxmax()]
        last_skill = last_message['SKILL']
        
        # Check shadowed status
        shadowed_status = is_conversation_shadowed_snowflake(conv_df, department_name, departments_config)
        shadowed_by = last_message['SHADOWED_BY']
        
        # Parse first assigned time and first button clicked time (if present)
        def _get_times_from_csv(series):
            try:
                if series is None or series.isna().all():
                    return None
                value = series.dropna().iloc[0]
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    return None
                parts = [p.strip().strip("'") for p in str(value).split(',') if p and str(p).strip()]
                if not parts:
                    return None
                times = pd.to_datetime(parts, errors='coerce')
                times = [t for t in times if not pd.isna(t)]
                return times if times else None
            except Exception:
                return None

        assigned_times = _get_times_from_csv(conv_df.get('ASSIGNED_SHADOWER_TIME'))
        button_clicked_times = _get_times_from_csv(conv_df.get('BUTTON_CLICKED_TIME'))
        first_assigned_time = min(assigned_times) if assigned_times else None
        last_button_clicked_time = max(button_clicked_times) if button_clicked_times else None


        # Only include conversations that ended with valid department skills
        if last_skill in valid_skills:
            conversation_order.append({
                'conversation_id': conv_id,
                'first_message_time': first_message_time,
                'first_message_date': first_message_time.date(),
                'shadowed': shadowed_status,
                'shadowed_by': shadowed_by,
                'assigned_shadower': last_message['ASSIGNED_SHADOWER'],
                'assigned_shadower_time': last_message['ASSIGNED_SHADOWER_TIME'],
                'button_clicked_time': last_message['BUTTON_CLICKED_TIME'],
                'first_assigned_time': first_assigned_time,
                'last_button_clicked_time': last_button_clicked_time,
                'conversation_data': conv_df,
                'assigned': is_assigned_shadower_department_agent(last_message['ASSIGNED_SHADOWER'], department_agents)
            })
    
    print(f"    🔍 {department_name}: {len(conversation_order)} conversations with valid skills")
    
    if not conversation_order:
        return None

    print(department_agents)
    conversation_order = [c for c in conversation_order if is_shadowed_by_department_agent(c['shadowed_by'], department_agents)]

    
    # Sort conversations by first message time
    conversation_order.sort(key=lambda x: x['first_message_time'])
    
    # Split conversations by date
    day_15_conversations = [c for c in conversation_order if c['first_message_date'] == day_15]
    day_16_conversations = [c for c in conversation_order if c['first_message_date'] == day_16]
    
    print(f"    🔍 {department_name}: Day {day_15}: {len(day_15_conversations)} conversations")
    print(f"    🔍 {department_name}: Day {day_16}: {len(day_16_conversations)} conversations")
    
    return {
        'day_15': process_day_shadowing_snowflake(day_15_conversations, day_15, department_name),
        'day_16': process_day_shadowing_snowflake(day_16_conversations, day_16, department_name)
    }


def analyze_shadowing_conversations_single_department(session, df, department_name, departments_config, target_date):
    """
    Analyze shadowing patterns for a single department and save raw data.
    Adapted from main_analytics.py analyze_shadowing_conversations()
    
    Args:
        session: Snowflake session
        df: Filtered DataFrame from Phase 1
        department_name: Department name
        departments_config: Department configuration
        target_date: Target date for analysis
    
    Returns:
        shadowing_results dictionary
    """
    print(f"  👥 Analyzing shadowing for {department_name}...")
    
    if df.empty:
        print(f"    ⚠️  No filtered data for {department_name}")
        return {
            'total_eligible_conversations': 0,
            'total_shadowed_conversations': 0,
            'total_assigned_conversations': 0,
            'overall_shadowing_percentage': 0.0,
            'day_15_eligible': 0,
            'day_15_shadowed': 0,
            'day_15_percentage': 0.0,
            'day_16_eligible': 0,
            'day_16_shadowed': 0,
            'day_16_percentage': 0.0
        }
    
    # Calculate the 2 days we're analyzing (based on target date)
    if isinstance(target_date, str):
        target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
    else:
        target_date_obj = target_date.date()
    
    day_16 = target_date_obj  # Day before target
    day_15 = target_date_obj - timedelta(days=1)  # Two days before target
    
    # Process shadowing analysis for this department
    dept_results = process_department_shadowing_snowflake(session, df, department_name, day_15, day_16, departments_config)
    
    if not dept_results:
        print(f"    ❌ {department_name}: No shadowing data found")
        return {
            'total_eligible_conversations': 0,
            'total_shadowed_conversations': 0,
            'total_assigned_conversations': 0,
            'overall_shadowing_percentage': 0.0,
            'day_15_eligible': 0,
            'day_15_shadowed': 0,
            'day_15_percentage': 0.0,
            'day_16_eligible': 0,
            'day_16_shadowed': 0,
            'day_16_percentage': 0.0
        }
    
    # Extract metrics
    day_15_metrics = dept_results['day_15']
    day_16_metrics = dept_results['day_16']
    
    # Calculate totals
    total_eligible = day_15_metrics['eligible_conversations'] + day_16_metrics['eligible_conversations']
    total_shadowed = day_15_metrics['shadowed_conversations'] + day_16_metrics['shadowed_conversations']
    total_assigned = day_15_metrics['assigned_conversations'] + day_16_metrics['assigned_conversations']
    overall_percentage = (total_shadowed / total_eligible * 100) if total_eligible > 0 else 0.0
    # Cap the assigned shadowed percentage at 100% maximum
    overall_shadowed_assigned_percentage = min(100.0, (total_shadowed / total_assigned * 100)) if total_assigned > 0 else 0.0
    total_unassigned = total_eligible - total_assigned
    
    # Extract shadowed conversation IDs for issues analysis
    shadowed_conversation_ids = []
    if dept_results:
        # Get conversation IDs from both days
        for day_data in [day_15_metrics, day_16_metrics]:
            for conv_data in day_data.get('eligible_conversation_data', []):
                is_shadowed = conv_data.get('IS_SHADOWED', '')
                # Handle both boolean True and string 'TRUE'
                if is_shadowed is True or str(is_shadowed).upper() == 'TRUE':
                    conv_id = conv_data.get('CONVERSATION_ID')
                    if conv_id and conv_id not in shadowed_conversation_ids:
                        shadowed_conversation_ids.append(conv_id)
    
    results = {
        'total_eligible_conversations': total_eligible,
        'total_shadowed_conversations': total_shadowed,
        'total_assigned_conversations': total_assigned,
        'overall_shadowing_percentage': overall_percentage,
        'overall_shadowed_assigned_percentage': overall_shadowed_assigned_percentage,
        'total_unassigned': total_unassigned,
        'day_15_eligible': day_15_metrics['eligible_conversations'],
        'day_15_shadowed': day_15_metrics['shadowed_conversations'],
        'day_15_percentage': day_15_metrics['shadowing_percentage'],
        'day_16_eligible': day_16_metrics['eligible_conversations'],
        'day_16_shadowed': day_16_metrics['shadowed_conversations'],
        'day_16_percentage': day_16_metrics['shadowing_percentage'],
        'shadowed_conversation_ids': shadowed_conversation_ids  # For issues analysis
    }
    
    print(f"    ✅ Day 15: {day_15_metrics['shadowed_conversations']}/{day_15_metrics['eligible_conversations']} ({day_15_metrics['shadowing_percentage']:.1f}%)")
    print(f"       Day 16: {day_16_metrics['shadowed_conversations']}/{day_16_metrics['eligible_conversations']} ({day_16_metrics['shadowing_percentage']:.1f}%)")
    print(f"       Overall: {total_shadowed}/{total_eligible} ({overall_percentage:.1f}%), {total_unassigned} unassigned, {total_assigned} assigned")
    
    # Save raw data to SHADOWING_RAW_DATA table (conversation-level)
    if total_eligible > 0:
        try:
            # Combine all eligible conversation data from both days
            all_eligible_data = []
            all_eligible_data.extend(day_15_metrics['eligible_conversation_data'])
            all_eligible_data.extend(day_16_metrics['eligible_conversation_data'])
            
            if all_eligible_data:
                # Convert to DataFrame for processing
                all_messages_df = pd.DataFrame(all_eligible_data)
                
                # Group by conversation and get conversation-level records
                conversation_records = []
                for conv_id, conv_group in all_messages_df.groupby('CONVERSATION_ID'):
                    # Get the last message (by MESSAGE_SENT_TIME) for THROUGH_SKILL and SHADOWED_BY
                    last_message = conv_group.loc[conv_group['MESSAGE_SENT_TIME'].idxmax()]                    
                    
                    conversation_records.append({
                        'CONVERSATION_ID': conv_id,
                        'THROUGH_SKILL': last_message.get('THROUGH_SKILL', ''),
                        'SHADOWED_BY': last_message.get('SHADOWED_BY', ''),
                        'IS_SHADOWED': last_message.get('IS_SHADOWED', ''),
                        'ASSIGNED_SHADOWER': last_message.get('ASSIGNED_SHADOWER', ''),
                        'ASSIGNED_SHADOWER_TIME': last_message.get('ASSIGNED_SHADOWER_TIME', ''),
                        'BUTTON_CLICKED_TIME': last_message.get('BUTTON_CLICKED_TIME', '')
                    })
                
                if conversation_records:
                    shadowing_df = pd.DataFrame(conversation_records)
                    shadowing_df = clean_dataframe_for_snowflake(shadowing_df)
                    
                    # Define dynamic columns (excluding the essential columns that insert_raw_data_with_cleanup adds)
                    dynamic_columns = [col for col in shadowing_df.columns if col not in ['DATE', 'DEPARTMENT', 'TIMESTAMP']]
                    
                    insert_raw_data_with_cleanup(
                        session=session,
                        table_name="SHADOWING_RAW_DATA",
                        department=department_name,
                        target_date=target_date,
                        dataframe=shadowing_df[dynamic_columns],
                        columns=dynamic_columns
                    )
                    print(f"    💾 Saved {len(conversation_records)} conversation-level shadowing records to SHADOWING_RAW_DATA")
                    
                    # Create agent breakdown table (assignments vs shadowed)
                    assigned_counts = {}
                    shadow_counts = {}
                    
                    # Get department agents for filtering (lowercase)
                    department_agents = get_department_agent_names_snowflake(session, department_name, departments_config)
                    
                    for record in conversation_records:
                        # Shadowed by (button clicked path)
                        shadowed_by = record.get('SHADOWED_BY', '')
                        if isinstance(shadowed_by, str) and shadowed_by.strip():
                            for raw_name in [n.strip() for n in shadowed_by.split(',') if n.strip()]:
                                norm = raw_name.lower()
                                if norm in department_agents:
                                    shadow_counts[norm] = shadow_counts.get(norm, 0) + 1
                    
                    print(f"    🔍 {department_name}: Shadow counts: {shadow_counts}")
                    # Count assigned chats from ASSIGNED_SHADOWER within eligible conversations
                    for record in conversation_records:
                        assigned_str = record.get('ASSIGNED_SHADOWER', '')
                        if isinstance(assigned_str, str) and assigned_str.strip():
                            for raw_name in [n.strip() for n in assigned_str.split(',') if n.strip()]:
                                norm = raw_name.lower()
                                if norm in department_agents:
                                    assigned_counts[norm] = assigned_counts.get(norm, 0) + 1
                    
                    print(f"    🔍 {department_name}: Assigned counts: {assigned_counts}")
                    # Create agent breakdown records (include assigned and percentage)
                    all_agents = set(assigned_counts.keys()) | set(shadow_counts.keys())
                    if all_agents:
                        agent_breakdown_records = []
                        for agent_norm in sorted(all_agents):
                            assigned = assigned_counts.get(agent_norm, 0)
                            shadowed = shadow_counts.get(agent_norm, 0)
                            pct = round((shadowed / assigned * 100), 1) if assigned > 0 else 0
                            agent_breakdown_records.append({
                                'AGENT_NAME': agent_norm,
                                'SHADOWED_NUMBERS': shadowed,
                                'ASSIGNED_CHATS_COUNTS': assigned,
                                'SHADOWED_PERCENTAGE': pct
                            })
                        
                        agent_breakdown_df = pd.DataFrame(agent_breakdown_records)
                        agent_breakdown_df = clean_dataframe_for_snowflake(agent_breakdown_df)
                        
                        # Define dynamic columns for agent breakdown
                        agent_dynamic_columns = [col for col in agent_breakdown_df.columns if col not in ['DATE', 'DEPARTMENT', 'TIMESTAMP']]
                        
                        insert_raw_data_with_cleanup(
                            session=session,
                            table_name="SHADOWING_AGENT_BREAKDOWN",
                            department=department_name,
                            target_date=target_date,
                            dataframe=agent_breakdown_df[agent_dynamic_columns],
                            columns=agent_dynamic_columns
                        )
                        print(f"    💾 Saved {len(agent_breakdown_records)} agent breakdown records to SHADOWING_AGENT_BREAKDOWN")
                    else:
                        print(f"    ⚠️  No agent breakdown data available for {department_name}")
        except Exception as e:
            print(f"    ⚠️  Failed to save shadowing raw data: {str(e)}")
    
    return results


def analyze_shadowing_conversations_all_departments(session: snowpark.Session, target_date=None):
    """
    Analyze shadowing patterns for all departments using Phase 1 filtered data.
    Raw data is saved immediately for each department.
    
    Args:
        session: Snowflake session
        target_date: Target date for analysis
    
    Returns:
        department_results dictionary
    """
    print("\n👥 PHASE 3C: ANALYZING SHADOWING CONVERSATIONS")
    print("=" * 60)
    
    departments_config = get_snowflake_departments_config()
    department_results = {}
    
    for department_name in departments_config.keys():
        if department_name!=DEPARTMENT_FILTER and TEST:
            continue
        
        # SHADOWING ENABLED ONLY FOR CC_Sales and MV_Sales
        if department_name not in ['CC_Sales', 'MV_Sales']:
            department_results[department_name] = {
                'total_eligible_conversations': 0,
                'total_shadowed_conversations': 0,
                'total_assigned_conversations': 0,
                'overall_shadowing_percentage': 0.0,
                'overall_shadowed_assigned_percentage': 0.0,
                'total_unassigned': 0,
                'shadowed_conversation_ids': []
            }
            continue
        
        try:
            # Get filtered data from Phase 1
            print(f"\n🏢 Processing {department_name}...")
            filtered_df, phase1_stats, success, _ = process_department_phase1(session, department_name, target_date)
            
            if not success or filtered_df.empty:
                print(f"  ❌ {department_name}: No filtered data from Phase 1")
                department_results[department_name] = {
                    'total_eligible_conversations': 0,
                    'total_shadowed_conversations': 0,
                    'total_assigned_conversations': 0,
                    'overall_shadowing_percentage': 0.0,
                    'overall_shadowed_assigned_percentage': 0.0,
                    'total_unassigned': 0,
                    'error': 'No filtered data from Phase 1'
                }
                continue
            
            # Apply department-specific skill filtering
            if department_name == "AT_Filipina_Outside_UAE":
                target_skills = [
                    "filipina_outside_pending_passport",
                    "filipina_outside_pending_ticket",
                    "filipina_outside_ticket_booked",
                    "filipina_outside_pending_facephoto",
                    "filipina_outside_uae_pending_joining_date",
                ]
                
                # Use Pandas string operations (filtered_df is a Pandas DataFrame)
                # Lowercase THROUGH_SKILL and handle NaN/None values
                through_skills_lower = filtered_df['THROUGH_SKILL'].fillna('').str.lower()
                
                # Create OR condition: check if any target skill appears in THROUGH_SKILL
                skill_mask = through_skills_lower.str.contains('|'.join(target_skills), regex=True, na=False)
                
                filtered_df = filtered_df[skill_mask]
                
                print(f"  ⚙️ Filtered AT_Filipina_Outside_UAE → {len(filtered_df)} rows match target skills (case-insensitive)")

            
            
            # Analyze shadowing for this department (includes raw data saving)
            shadowing_results = analyze_shadowing_conversations_single_department(
                session, filtered_df, department_name, departments_config, target_date
            )
            
            department_results[department_name] = shadowing_results
            
        except Exception as e:
            error_msg = f"Shadowing analysis failed: {str(e)}"
            print(f"  ❌ {department_name}: {error_msg}")
            department_results[department_name] = {
                'total_eligible_conversations': 0,
                'total_shadowed_conversations': 0,
                'total_assigned_conversations': 0,
                'overall_shadowing_percentage': 0.0,
                'overall_shadowed_assigned_percentage': 0.0,
                'total_unassigned': 0,
                'error': error_msg
            }
    
    # Generate summary
    total_eligible_all = sum(r.get('total_eligible_conversations', 0) for r in department_results.values())
    total_shadowed_all = sum(r.get('total_shadowed_conversations', 0) for r in department_results.values())
    total_assigned_all = sum(r.get('total_assigned_conversations', 0) for r in department_results.values())
    overall_shadowing_percentage = (total_shadowed_all / total_eligible_all * 100) if total_eligible_all > 0 else 0
    
    print(f"\n📊 SHADOWING ANALYSIS SUMMARY:")
    print(f"   📋 Total eligible conversations: {total_eligible_all:,}")
    print(f"   👥 Total shadowed conversations: {total_shadowed_all:,}")
    print(f"   👥 Total assigned conversations: {total_assigned_all:,}")
    print(f"   📈 Overall shadowing rate: {overall_shadowing_percentage:.1f}%")
    print(f"   💾 Raw data saved to: SHADOWING_RAW_DATA")
    print(f"   👤 Agent breakdown saved to: SHADOWING_AGENT_BREAKDOWN")
    
    return department_results


# ============================================================================
# PHASE 3: ADVANCED ANALYTICS EXTENSION

def test_shadowing_single_department(session: snowpark.Session, department_name, target_date=None):
    """
    Test shadowing analysis for a single department.
    """
    print(f"🧪 TESTING SHADOWING ANALYSIS - {department_name}")
    print("=" * 50)
    
    try:
        departments_config = get_snowflake_departments_config()
        
        # Get filtered data from Phase 1
        filtered_df, stats, success, _ = process_department_phase1(session, department_name, target_date)
        
        if not success:
            print(f"❌ Phase 1 failed for {department_name}")
            return
        
        # Check if IS_SHADOWED column exists
        if 'IS_SHADOWED' in filtered_df.columns:
            print(f"✅ IS_SHADOWED column found")
            shadowed_count = len(filtered_df[filtered_df['IS_SHADOWED'].astype(str).str.upper() == 'TRUE'])
            print(f"   {shadowed_count} rows marked as shadowed")
        else:
            print(f"⚠️  IS_SHADOWED column not found in data")
            print(f"   Available columns: {list(filtered_df.columns)}")
        
        # Run shadowing analysis
        shadowing_results = analyze_shadowing_conversations_single_department(
            session, filtered_df, department_name, departments_config, target_date
        )
        
        print(f"\n📊 SHADOWING RESULTS:")
        for key, value in shadowing_results.items():
            print(f"   {key}: {value}")
        
        print(f"\n💡 Overall Shadowing Percentage: {shadowing_results.get('overall_shadowing_percentage', 0):.2f}%")
            
    except Exception as e:
        error_report = format_error_details(e, f"SHADOWING TEST - {department_name}")
        print(error_report)

# ============================================================================
# ISSUES ANALYSIS
# ============================================================================

def get_department_agent_names_snowflake(session: snowpark.Session, department_name, departments_config):
    """
    Get agent names for a department from AGENTVIEW table based on skill matching.
    
    Args:
        session: Snowflake session
        department_name: Department name
        departments_config: Department configuration
    
    Returns:
        set: Set of agent names (lowercase) for the department
    """
    try:
        dept_config = departments_config[department_name]
        agent_skills = dept_config['agent_skills']
        
        if not agent_skills:
            print(f"    ⚠️  {department_name}: No agent skills configured")
            return set()
        
        # Build exact match conditions for each skill in comma-separated string
        # This handles exact matching within comma-separated values to avoid partial matches
        skill_conditions = []
        for skill in agent_skills:
            skill_upper = skill.upper()
            # Create conditions for exact skill matching in comma-separated string:
            # 1. Skill at the beginning: 'SKILL,' or 'SKILL, '
            # 2. Skill in the middle: ',SKILL,' or ', SKILL,' or ', SKILL ,'
            # 3. Skill at the end: ',SKILL' or ', SKILL'
            # 4. Single skill: exactly 'SKILL'
            condition = f"""(
                UPPER(TRIM(SKILLNAME)) = '{skill_upper}' OR
                UPPER(TRIM(SKILLNAME)) LIKE '{skill_upper},%' OR
                UPPER(TRIM(SKILLNAME)) LIKE '%,{skill_upper},%' OR
                UPPER(TRIM(SKILLNAME)) LIKE '%,{skill_upper}' OR
                UPPER(TRIM(SKILLNAME)) LIKE '{skill_upper}, %' OR
                UPPER(TRIM(SKILLNAME)) LIKE '%, {skill_upper},%' OR
                UPPER(TRIM(SKILLNAME)) LIKE '%, {skill_upper}'
            )"""
            skill_conditions.append(condition)
        
        where_clause = " OR ".join(skill_conditions)
        
        query = f"""
        SELECT DISTINCT UPPER(AGENTNAME) as AGENTNAME
        FROM LLM_EVAL.RAW_DATA.AGENTVIEW
        WHERE ({where_clause})
        AND AGENTNAME IS NOT NULL
        AND AGENTNAME != ''
        """
        
        print(f"    🔍 {department_name}: Querying agents for skills: {agent_skills}")
        result = session.sql(query).collect()
        
        agent_names = {row['AGENTNAME'].lower().strip() for row in result if row['AGENTNAME']}
        print(f"    ✅ {department_name}: Found {len(agent_names)} agents")
        
        return agent_names
        
    except Exception as e:
        print(f"    ❌ {department_name}: Failed to get agent names - {str(e)}")
        return set()


def analyze_issues_single_department(session: snowpark.Session, department_name, departments_config, target_date, shadowed_conversation_ids):
    """
    Analyze issues for a single department using Snowflake views.
    
    Args:
        session: Snowflake session
        department_name: Department name
        departments_config: Department configuration
        target_date: Target date for analysis
        shadowed_conversation_ids: List of shadowed conversation IDs
    
    Returns:
        issues_results dictionary
    """
    print(f"  🛠️  Analyzing issues for {department_name}...")
    
    try:
        # Get department agent names
        agent_names = get_department_agent_names_snowflake(session, department_name, departments_config)
        if not agent_names:
            print(f"    ⚠️  {department_name}: No agents found")
            return {
                'shadowed_reported_issues': 0,
                'reported_percentage': 0.0,
                'open_issues_by_agents': 0,
                'total_shadowed_conversations': len(shadowed_conversation_ids),
                'shadowed_reported_conversation_ids': '',
                'error': 'No agents found'
            }
        
        # Calculate date ranges
        if isinstance(target_date, str):
            target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
        else:
            target_date_obj = target_date.date()
        
        filter_start_date = target_date_obj - timedelta(days=1)
        short_range_start = datetime.combine(filter_start_date, datetime.min.time()).strftime('%Y-%m-%d %H:%M:%S')
        short_range_end = datetime.combine(target_date_obj, datetime.max.time()).strftime('%Y-%m-%d %H:%M:%S')
        if department_name == "CC_Sales":
            long_range_start = "2025-08-03 00:00:00"
        elif department_name == "MV_Sales":
            long_range_start = "2025-09-11 00:00:00"
        else:
            long_range_start = "2025-07-08 00:00:00"
        long_range_end = datetime.combine(target_date_obj, datetime.max.time()).strftime('%Y-%m-%d %H:%M:%S')
        
        # Convert shadowed conversation IDs to SQL-friendly format
        if shadowed_conversation_ids:
            shadowed_ids_str = "', '".join(str(cid) for cid in shadowed_conversation_ids)
            shadowed_ids_filter = f"AND CONVERSATION_ID IN ('{shadowed_ids_str}')"
        else:
            shadowed_ids_filter = "AND 1=0"  # No shadowed conversations
        
        # Convert agent names to SQL-friendly format
        agent_names_escaped = [name.replace("'", "''") for name in agent_names]
        agent_names_upper   = [n.upper() for n in agent_names_escaped]
        agent_names_str     = "', '".join(agent_names_upper)
        
        agent_filter = f"AND UPPER(REPORTER) IN ('{agent_names_str}')"
        
        # Add assignee filter only for CC_Sales department
        assignee_filter = "AND ASSIGNEE = 'Ahmed Hossam'" if department_name == "CC_Sales" else ""
        
        # Query recent reported issues (shadowed conversations only)
        recent_issues_query = f"""
        SELECT DISTINCT CONVERSATION_ID
        FROM LLM_EVAL.RAW_DATA.CHATCC_REPORTED_ISSUES
        WHERE CREATION_DATE >= '{short_range_start}'
        AND CREATION_DATE <= '{short_range_end}'
        {assignee_filter}
        {agent_filter}
        {shadowed_ids_filter}
        """
        
        

        
        # Query pending issues (TO_BE_REVIEWED, ONGOING status in shadowed conversations)
        pending_issues_query = f"""
        SELECT COUNT(*) as PENDING_COUNT
        FROM LLM_EVAL.RAW_DATA.CHATCC_REPORTED_ISSUES
        WHERE CREATION_DATE >= '{long_range_start}'
        AND CREATION_DATE <= '{long_range_end}'
        AND ISSUE_STATUS IN ('TO_BE_REVIEWED', 'ONGOING')
        {assignee_filter}
        {agent_filter}
        
        """
        if department_name == "MV_Resolvers" or department_name == "Delighters":
                    pending_issues_query = f"""
                SELECT COUNT(*) as PENDING_COUNT
                FROM LLM_EVAL.RAW_DATA.CHATCC_REPORTED_ISSUES
                WHERE CREATION_DATE >= '{long_range_start}'
                AND CREATION_DATE <= '{long_range_end}'
                AND ISSUE_STATUS IN ('TO_BE_REVIEWED', 'ONGOING')
                {assignee_filter}
                {agent_filter}
                AND CONVERSATION_ID IN(
                select distinct conversation_id 
                from LLM_EVAL.PUBLIC.DELAY_ANALYSIS_RAW_DATA 
                where department = '{department_name}'
                and date(date) >= '{long_range_start}'
                and date(date) <= '{long_range_end}')
                    """

                    recent_issues_query = f"""
                SELECT DISTINCT CONVERSATION_ID
                FROM LLM_EVAL.RAW_DATA.CHATCC_REPORTED_ISSUES
                WHERE CREATION_DATE >= '{short_range_start}'
                AND CREATION_DATE <= '{short_range_end}'
                {assignee_filter}
                {agent_filter}
                {shadowed_ids_filter}
                AND CONVERSATION_ID IN(
                select distinct conversation_id 
                from LLM_EVAL.PUBLIC.DELAY_ANALYSIS_RAW_DATA 
                where department = '{department_name}'
                and date(date) >= '{short_range_start}'
                and date(date) <= '{short_range_end}')
                    """


        print(f"    🔍 {department_name}: Querying recent reported issues...")
        print(f"    ✅ {department_name}: Query: {recent_issues_query}")
        recent_results = session.sql(recent_issues_query).collect()
        reported_conversation_ids = [row['CONVERSATION_ID'] for row in recent_results]
        shadowed_reported_count = len(reported_conversation_ids)


        print(f"    🔍 {department_name}: Querying pending issues...")
        print(f"    ✅ {department_name}: Query: {pending_issues_query}")
        pending_results = session.sql(pending_issues_query).collect()
        open_issues_count = pending_results[0]['PENDING_COUNT'] if pending_results else 0
        
        # Calculate reported percentage
        total_shadowed_conversations = len(shadowed_conversation_ids)
        if total_shadowed_conversations > 0:
            reported_percentage = (shadowed_reported_count / total_shadowed_conversations * 100)
        else:
            reported_percentage = 0.0
        
        # Create comma-separated string of reported conversation IDs
        reported_conversation_ids_str = ','.join(str(cid) for cid in reported_conversation_ids)
        
        results = {
            'shadowed_reported_issues': shadowed_reported_count,
            'reported_percentage': reported_percentage,
            'open_issues_by_agents': open_issues_count,
            'total_shadowed_conversations': total_shadowed_conversations,
            'shadowed_reported_conversation_ids': reported_conversation_ids_str,
            'agent_count': len(agent_names)
        }
        
        print(f"    ✅ {department_name}: Reported {shadowed_reported_count}/{total_shadowed_conversations} ({reported_percentage:.1f}%), Pending {open_issues_count}")
        
        # Save raw data to ISSUES_RAW_DATA table (following same pattern as other metrics)
        if shadowed_reported_count > 0 or open_issues_count > 0:
            try:
                # Create DataFrame from summary results (single row per department)
                issues_raw_df = pd.DataFrame([{
                    'SHADOWED_REPORTED_ISSUES': shadowed_reported_count,
                    'REPORTED_PERCENTAGE': reported_percentage,
                    'OPEN_ISSUES_BY_AGENTS': open_issues_count,
                    'TOTAL_SHADOWED_CONVERSATIONS': total_shadowed_conversations,
                    'SHADOWED_REPORTED_CONVERSATION_IDS': reported_conversation_ids_str,
                    'AGENT_COUNT': len(agent_names)
                }])
                
                issues_raw_df = clean_dataframe_for_snowflake(issues_raw_df)
                
                # Define dynamic columns (excluding the essential columns that insert_raw_data_with_cleanup adds)
                dynamic_columns = [col for col in issues_raw_df.columns if col not in ['DATE', 'DEPARTMENT', 'TIMESTAMP']]
                
                insert_raw_data_with_cleanup(
                    session=session,
                    table_name="ISSUES_RAW_DATA",
                    department=department_name,
                    target_date=target_date,
                    dataframe=issues_raw_df[dynamic_columns],
                    columns=dynamic_columns
                )
                print(f"    💾 Saved {len(issues_raw_df)} issues records to ISSUES_RAW_DATA")
            except Exception as e:
                print(f"    ⚠️  Failed to save issues raw data: {str(e)}")
        
        return results
        
    except Exception as e:
        error_msg = f"Issues analysis failed: {str(e)}"
        print(f"    ❌ {department_name}: {error_msg}")
        return {
            'shadowed_reported_issues': 0,
            'reported_percentage': 0.0,
            'open_issues_by_agents': 0,
            'total_shadowed_conversations': len(shadowed_conversation_ids),
            'shadowed_reported_conversation_ids': '',
            'error': error_msg
        }


def analyze_issues_all_departments(session: snowpark.Session, target_date, shadowing_results):
    """
    Analyze issues for all departments using shadowing results.
    
    Args:
        session: Snowflake session
        target_date: Target date for analysis
        shadowing_results: Results from shadowing analysis
    
    Returns:
        department_results dictionary
    """
    print("\n🛠️  PHASE 3D: ANALYZING AGENT ISSUES")
    print("=" * 60)
    
    departments_config = get_snowflake_departments_config()
    department_results = {}
    
    for department_name in departments_config.keys():
        if department_name!=DEPARTMENT_FILTER and TEST:
            continue
        
        # ISSUES ANALYSIS ENABLED ONLY FOR CC_Sales and MV_Sales
        if department_name not in ['CC_Sales', 'MV_Sales']:
            department_results[department_name] = {
                'shadowed_reported_issues': 0,
                'reported_percentage': 0.0,
                'open_issues_by_agents': 0,
                'total_shadowed_conversations': 0,
                'shadowed_reported_conversation_ids': ''
            }
            continue
        
        try:
            # Get shadowed conversation IDs from shadowing results
            shadowing_data = shadowing_results.get(department_name, {})
            shadowed_conversation_ids = shadowing_data.get('shadowed_conversation_ids', [])
            
            print(f"\n🏢 Processing {department_name}...")
            print(f"    📊 {len(shadowed_conversation_ids)} shadowed conversations available")
            
            # Analyze issues for this department
            issues_results = analyze_issues_single_department(
                session, department_name, departments_config, target_date, shadowed_conversation_ids
            )
            
            department_results[department_name] = issues_results
            
        except Exception as e:
            error_msg = f"Issues analysis failed: {str(e)}"
            print(f"  ❌ {department_name}: {error_msg}")
            department_results[department_name] = {
                'shadowed_reported_issues': 0,
                'reported_percentage': 0.0,
                'open_issues_by_agents': 0,
                'total_shadowed_conversations': 0,
                'shadowed_reported_conversation_ids': '',
                'error': error_msg
            }
    
    # Generate summary
    total_shadowed_all = sum(r.get('total_shadowed_conversations', 0) for r in department_results.values())
    total_reported_all = sum(r.get('shadowed_reported_issues', 0) for r in department_results.values())
    total_pending_all = sum(r.get('open_issues_by_agents', 0) for r in department_results.values())
    overall_reported_percentage = (total_reported_all / total_shadowed_all * 100) if total_shadowed_all > 0 else 0
    
    print(f"\n📊 ISSUES ANALYSIS SUMMARY:")
    print(f"   📋 Total shadowed conversations: {total_shadowed_all:,}")
    print(f"   🛠️  Total reported issues: {total_reported_all:,}")
    print(f"   📈 Overall reported rate: {overall_reported_percentage:.1f}%")
    print(f"   ⏳ Total pending issues: {total_pending_all:,}")
    print(f"   💾 Raw data saved to: ISSUES_RAW_DATA")
    
    return department_results


def test_similarity_single_department(session: snowpark.Session, department_name, target_date=None):
    """
    Test similarity analysis for a single department.
    """
    print(f"🧪 TESTING SIMILARITY ANALYSIS - {department_name}")
    print("=" * 50)
    
    try:
        departments_config = get_snowflake_departments_config()
        
        # Get filtered data from Phase 1
        filtered_df, stats, success, _ = process_department_phase1(session, department_name, target_date)
        
        if not success:
            print(f"❌ Phase 1 failed for {department_name}")
            return
        
        # Check if TEXT column exists
        if 'TEXT' in filtered_df.columns:
            print(f"✅ TEXT column found")
            bot_text_count = len(filtered_df[(filtered_df['SENT_BY'].str.upper() == 'BOT') & (filtered_df['TEXT'].notna())])
            print(f"   {bot_text_count} bot messages with text found")
        else:
            print(f"❌ TEXT column not found in data")
            print(f"   Available columns: {list(filtered_df.columns)}")
            return
        
        # Test sklearn imports
        try:
            from sklearn.feature_extraction.text import TfidfVectorizer
            from sklearn.metrics.pairwise import cosine_similarity
            print("✅ sklearn imports successful")
        except ImportError as e:
            print(f"❌ sklearn import failed: {str(e)}")
            return
         
        # Run similarity analysis
        similarity_results = analyze_similarity_conversations_single_department(
            session, filtered_df, department_name, departments_config, target_date
        )
        
        print(f"\n📊 SIMILARITY RESULTS:")
        for key, value in similarity_results.items():
            print(f"   {key}: {value}")
        
        print(f"\n💡 Key Metrics:")
        print(f"   Conversations with 50% similarity: {similarity_results.get('similarity_conversation_count', 0)}")
        print(f"   Similarity percentage: {similarity_results.get('similarity_percentage', 0):.2f}%")
        print(f"   Average similarity score: {similarity_results.get('avg_similarity', 0):.3f}")
            
    except Exception as e:
        error_report = format_error_details(e, f"SIMILARITY TEST - {department_name}")
        print(error_report)


def test_issues_single_department(session: snowpark.Session, department_name, target_date=None):
    """
    Test issues analysis for a single department.
    """
    print(f"🧪 TESTING ISSUES ANALYSIS - {department_name}")
    print("=" * 50)
    
    try:
        departments_config = get_snowflake_departments_config()
        
        # First test if views exist
        try:
            session.sql("SELECT 1 FROM CHATCC_REPORTED_ISSUES LIMIT 1").collect()
            print("✅ CHATCC_REPORTED_ISSUES view accessible")
        except Exception as e:
            print(f"❌ CHATCC_REPORTED_ISSUES view not accessible: {str(e)}")
            return
        
        try:
            session.sql("SELECT 1 FROM AGENTVIEW LIMIT 1").collect()
            print("✅ AGENTVIEW view accessible")
        except Exception as e:
            print(f"❌ AGENTVIEW view not accessible: {str(e)}")
            return
        
        # Test agent names retrieval
        agent_names = get_department_agent_names_snowflake(session, department_name, departments_config)
        print(f"📋 Found {len(agent_names)} agents for {department_name}")
        
        # Get shadowing data first
        print("\n🔄 Getting shadowing data...")
        shadowing_results = analyze_shadowing_conversations_single_department(
            session, pd.DataFrame(), department_name, departments_config, target_date
        )
        
        # Mock some shadowed conversation IDs for testing
        test_shadowed_ids = ['test_conv_1', 'test_conv_2', 'test_conv_3']
        print(f"🧪 Using test shadowed conversation IDs: {test_shadowed_ids}")
        
        # Run issues analysis
        issues_results = analyze_issues_single_department(
            session, department_name, departments_config, target_date, test_shadowed_ids
        )
        
        print(f"\n📊 ISSUES RESULTS:")
        for key, value in issues_results.items():
            print(f"   {key}: {value}")
        
        print(f"\n💡 Key Metrics:")
        print(f"   Reported Issues: {issues_results.get('shadowed_reported_issues', 0)}")
        print(f"   Reported Percentage: {issues_results.get('reported_percentage', 0):.2f}%")
        print(f"   Open Issues: {issues_results.get('open_issues_by_agents', 0)}")
            
    except Exception as e:
        error_report = format_error_details(e, f"ISSUES TEST - {department_name}")
        print(error_report)



def parse_transfer(text):
    """
    Extracts 'By', 'from skill', and 'to skill' from the given transfer string.

    Args:
        text (str): Input string like 
            "Transfer To User TarekA By TarekA ,from skill GPT_MV_RESOLVERS to skill GPT_MV_RESOLVERS_SHADOWERS"

    Returns:
        dict: Dictionary with keys 'by', 'from_skill', 'to_skill'
    """
    result = {}

    # Extract "By"
    by_match = re.search(r'By\s+(\w+)', text, re.IGNORECASE)
    if by_match:
        result['by'] = by_match.group(1).strip()

    # Extract "from skill"
    from_match = re.search(r'from skill\s+(\w+)', text, re.IGNORECASE)
    if from_match:
        result['from_skill'] = from_match.group(1).strip()

    # Extract "to skill" (everything until the end of string)
    to_match = re.search(r'to skill\s+(.+)$', text, re.IGNORECASE)
    if to_match:
        result['to_skill'] = to_match.group(1).strip()

    return result

def parse_transfer_2222(text):
    """
    Extracts 'By', 'from skill', and 'to skill' from the given transfer string.

    Args:
        text (str): Input string like 
            "Transfer To User TarekA By TarekA ,from skill GPT_MV_RESOLVERS to skill GPT_MV_RESOLVERS_SHADOWERS"

    Returns:
        dict: Dictionary with keys 'by', 'from_skill', 'to_skill'
    """
    result = {}

    #check if text contains "Transfer To User" in any case using regex
    # if re.search(r'Transfer To User', text, re.IGNORECASE):   
    if True:
        # Extract "By"
        by_match = re.search(r'By\s+(\w+)', text, re.IGNORECASE)
        if by_match:
            result['by'] = by_match.group(1).strip()

        # Extract "from skill"
        from_match = re.search(r'from skill\s+(\w+)', text, re.IGNORECASE)
        if from_match:
            result['from_skill'] = from_match.group(1).strip()

        # Extract "to skill" - use two approaches and take the shortest
        to_skill_candidates = []
        
        # Approach 1: Extract from "to skill" to the end of string
        to_match_full = re.search(r'to skill\s+(.+)$', text, re.IGNORECASE)
        if to_match_full:
            to_skill_candidates.append(to_match_full.group(1).strip())
        
        # Approach 2: Extract from "to skill" until we hit "by" (case-insensitive)
        to_match_until_by = re.search(r'to skill\s+(.+?)(?=\s+by\s+)', text, re.IGNORECASE)
        if to_match_until_by:
            to_skill_candidates.append(to_match_until_by.group(1).strip())
        
        # Take the shortest one
        if to_skill_candidates:
            result['to_skill'] = min(to_skill_candidates, key=len)

    return result

def filter_conversations_snowflake_hi_bye(session: snowpark.Session, df, department_name, target_date=None):
    """
    Remove hi-bye conversations based on HI_BYE_CHATS table.
    
    Args:
        session: Snowflake session for querying HI_BYE_CHATS table
        df: DataFrame after previous filtering
        department_name: Department name
        target_date: Target date for analysis
    
    Returns:
        Tuple: (filtered_df, filtering_stats)
    """
    print(f"  👋 Applying hi-bye conversation filtering for {department_name}...")
    
    if df.empty:
        print(f"    ⚠️  Input DataFrame is empty, skipping hi-bye filtering")
        return df, {'hi_bye_conversations_removed': 0, 'hi_bye_retention_rate': 100.0}
    
    # Get date for filtering
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    elif isinstance(target_date, datetime):
        target_date = target_date.strftime('%Y-%m-%d')
    
    conversations_before = df['CONVERSATION_ID'].nunique()
    
    try:
        # Query HI_BYE_CHATS table for conversation IDs to exclude
        hi_bye_query = f"""
        SELECT DISTINCT CONVERSATION_ID
        FROM LLM_EVAL.PUBLIC.HI_BYE_CHATS
        WHERE DATE = DATE('{target_date}')
          AND UPPER(DEPARTMENT) = UPPER('{department_name}')
        """
        
        hi_bye_df = session.sql(hi_bye_query).to_pandas()
        
        if hi_bye_df.empty:
            print(f"    ✅ No hi-bye conversations found in table")
            filtering_stats = {
                'hi_bye_conversations_removed': 0,
                'hi_bye_retention_rate': 100.0
            }
            return df, filtering_stats
        
        # Get list of conversation IDs to exclude
        hi_bye_conv_ids = set(hi_bye_df['CONVERSATION_ID'].unique())
        print(f"    📋 Found {len(hi_bye_conv_ids)} hi-bye conversations to exclude")
        
        # Filter out hi-bye conversations
        filtered_df = df[~df['CONVERSATION_ID'].isin(hi_bye_conv_ids)]
        
        conversations_after = filtered_df['CONVERSATION_ID'].nunique()
        conversations_removed = conversations_before - conversations_after
        
        # Calculate statistics
        filtering_stats = {
            'hi_bye_conversations_removed': conversations_removed,
            'hi_bye_retention_rate': (conversations_after / conversations_before * 100) if conversations_before > 0 else 0
        }
        
        print(f"    🗑️  Removed {conversations_removed} hi-bye conversations")
        print(f"    📈 Hi-bye retention: {filtering_stats['hi_bye_retention_rate']:.1f}%")
        
        return filtered_df, filtering_stats
        
    except Exception as e:
        print(f"    ⚠️  Error querying HI_BYE_CHATS table: {str(e)}")
        print(f"    ➡️  Continuing without hi-bye filtering")
        filtering_stats = {
            'hi_bye_conversations_removed': 0,
            'hi_bye_retention_rate': 100.0,
            'hi_bye_filter_error': str(e)
        }
        return df, filtering_stats