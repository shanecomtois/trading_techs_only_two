"""
OpenAI API client for AI trade alignment analysis.
Handles API key loading and OpenAI API calls.
"""
import os
import json
import logging
from pathlib import Path
from typing import Dict, Optional
import time

# Try to import openai library
try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    logging.warning("OpenAI library not installed. Install with: pip install openai")

logger = logging.getLogger(__name__)

# Default API key file path (relative to project root)
DEFAULT_API_KEY_PATH = Path(__file__).parent.parent.parent / 'AI' / 'openai_api_key.txt'


def load_openai_config(api_key_path: Optional[str] = None) -> Optional[str]:
    """
    Load OpenAI API key from file or environment variable.
    
    Args:
        api_key_path: Path to API key file (default: AI/openai_api_key.txt)
    
    Returns:
        API key string, or None if not found
    """
    # First, try environment variable
    api_key = os.getenv('OPENAI_API_KEY')
    if api_key:
        logger.info("Loaded OpenAI API key from environment variable")
        return api_key.strip()
    
    # If not in environment, try file
    if api_key_path is None:
        api_key_path = DEFAULT_API_KEY_PATH
    else:
        api_key_path = Path(api_key_path)
    
    if not api_key_path.exists():
        logger.error(f"OpenAI API key file not found: {api_key_path}")
        logger.error("Please set OPENAI_API_KEY environment variable or create the API key file")
        return None
    
    try:
        with open(api_key_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    if key.strip() == 'OPENAI_API_KEY':
                        api_key = value.strip()
                        logger.info(f"Loaded OpenAI API key from file: {api_key_path}")
                        return api_key
        
        logger.error(f"No OPENAI_API_KEY found in file: {api_key_path}")
        return None
    except Exception as e:
        logger.error(f"Error loading OpenAI API key from {api_key_path}: {e}")
        return None


def get_ai_trade_alignment(
    trade_payload: Dict,
    api_key: Optional[str] = None,
    model: str = "gpt-4",
    temperature: float = 0.3,
    max_tokens: int = 1000,
    timeout: int = 30,
    multi_pass: bool = True,
    num_passes: int = 3
) -> Dict:
    """
    Call OpenAI API to get AI trade alignment assessment.
    
    Args:
        trade_payload: Structured trade payload dictionary (see design doc)
        api_key: OpenAI API key (if None, will try to load from config)
        model: OpenAI model to use (default: "gpt-4")
        temperature: Temperature for response (default: 0.3 for analytical responses)
        max_tokens: Maximum tokens in response (default: 1000)
        timeout: Request timeout in seconds (default: 30)
    
    Returns:
        Dictionary with AI response:
        {
            "alignment_label": str,
            "technical_view": str,
            "fundamental_view": str,
            "overall_comment": str,
            "confidence": int
        }
        Or error response:
        {
            "error": str,
            "alignment_label": "AI Error"
        }
    """
    if not OPENAI_AVAILABLE:
        logger.error("OpenAI library not available. Install with: pip install openai")
        return {
            "error": "OpenAI library not installed",
            "alignment_label": "AI Error",
            "technical_view": "",
            "fundamental_view": "",
            "overall_comment": "",
            "confidence": 0
        }
    
    # Load API key if not provided
    if api_key is None:
        api_key = load_openai_config()
        if api_key is None:
            return {
                "error": "OpenAI API key not found",
                "alignment_label": "AI Config Error",
                "technical_view": "",
                "fundamental_view": "",
                "overall_comment": "",
                "confidence": 0
            }
    
    # Initialize OpenAI client
    try:
        client = openai.OpenAI(api_key=api_key)
    except Exception as e:
        logger.error(f"Error initializing OpenAI client: {e}")
        return {
            "error": f"OpenAI client initialization failed: {e}",
            "alignment_label": "AI Error",
            "technical_view": "",
            "fundamental_view": "",
            "overall_comment": "",
            "confidence": 0
        }
    
    # Build system message
    system_message = """You are a senior energy commodity analyst with deep expertise in:
- Energy commodity markets (crude oil, refined products, NGLs, natural gas)
- Technical analysis and trading signals
- Spread trading (inter-commodity, inter-tenor, calendar spreads)
- Fundamental analysis of supply/demand, seasonality, and market structure
- Historical market conditions and price dynamics

Your task is to RESEARCH and BALANCED evaluate trade signals as a true analyst would.

BALANCED ANALYST APPROACH:
1. RESEARCH THE MARKET: Use your knowledge of energy markets to understand what was happening at the trade date/timeframe
   - Consider: seasonal patterns, supply/demand fundamentals, geopolitical events, inventory levels, refinery runs
   - Think about: What were typical spreads at that time? What were seasonal expectations?
   - Analyze: Are the commodities in the trade fundamentally aligned or misaligned for that timeframe?
   - Use forward curve data to assess market structure (contango/backwardation)

2. BE BALANCED AND HONEST:
   - Use the FULL RANGE of alignment labels appropriately: Strongly Agree, Agree, Neutral, Disagree, Strongly Disagree
   - AGREE when: Technical indicators are strong (high score, multiple confirmations) AND fundamentals/market structure support the trade
   - DISAGREE when: There are clear fundamental mismatches, poor risk/reward, or market structure contradicts the trade
   - Don't be overly negative - recognize when a trade has merit despite some concerns
   - A trade with strong technicals (score 95+) and reasonable fundamentals should often be "Agree" or "Neutral", not automatically "Disagree"

3. EVALUATION CRITERIA:
   - Strong technicals (score 95-110) + supportive fundamentals = Agree/Strongly Agree
   - Strong technicals + neutral/mixed fundamentals = Neutral/Agree
   - Strong technicals + clearly negative fundamentals = Disagree
   - Weak technicals OR poor risk/reward = Disagree/Strongly Disagree
   - Give credit where due - if technicals are excellent and fundamentals are reasonable, don't default to Disagree

CRITICAL UNDERSTANDING OF SPREADS:
- Spreads have two legs: Leg 1 and Leg 2
- A "Buy" spread means: Buy Leg 1 / Sell Leg 2
- A "Sell" spread means: Sell Leg 1 / Buy Leg 2
- Legs can involve different commodities and different tenors (including quarterlies like Q1, Q2, Q3, Q4)
- Outrights have only one leg (Leg 1), and the signal direction applies directly to it

You must evaluate each trade from both:
1. Technical perspective: Based on the score details, indicators, and risk metrics provided
2. Fundamental perspective: Based on your RESEARCH of market conditions, commodities, tenors, and seasonality
3. Market structure perspective: Analyze the forward curves provided
   - CONTANGO (prices rising over time): BEARISH structure - indicates oversupply/storage costs, bearish for longs
   - BACKWARDATION (prices falling over time): BULLISH structure - indicates tight supply/demand, bullish for longs
   - For BUY signals: Backwardation is supportive, Contango is bearish/negative
   - For SELL signals: Contango is supportive, Backwardation is bullish/negative
   - Consider if the trade direction aligns with the forward curve structure

Provide your assessment as JSON only, with no additional text."""

    # Build user message from trade_payload
    user_message = _build_user_message(trade_payload)
    
    # Multi-pass system for accuracy
    if multi_pass and num_passes > 1:
        return _get_ai_alignment_multipass(
            trade_payload, api_key, model, temperature, max_tokens, timeout, num_passes
        )
    
    # Single-pass (original behavior)
    # Make API call
    start_time = time.time()
    try:
        # Note: response_format={"type": "json_object"} only works with certain models (gpt-4-turbo, gpt-4o)
        # For gpt-4, we'll request JSON in the prompt and parse it manually
        use_json_format = model in ["gpt-4-turbo", "gpt-4-turbo-preview", "gpt-4o", "gpt-4o-mini"]
        
        # Enhance user message to strongly request JSON if not using response_format
        if not use_json_format:
            user_message += "\n\nIMPORTANT: Respond ONLY with valid JSON. Do not include any text before or after the JSON object."
        
        api_params = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_message},
                {"role": "user", "content": user_message}
            ],
            "temperature": temperature,
            "max_tokens": max_tokens
        }
        
        if use_json_format:
            api_params["response_format"] = {"type": "json_object"}
        
        response = client.chat.completions.create(**api_params)
        
        duration = time.time() - start_time
        logger.info(f"OpenAI API call completed in {duration:.2f}s")
        
        # Extract response content
        response_content = response.choices[0].message.content
        
        # Parse JSON response - handle cases where response might have extra text
        try:
            # Try to extract JSON from response (in case model adds extra text)
            response_content_clean = response_content.strip()
            
            # If response starts with ```json or ```, extract JSON part
            if response_content_clean.startswith("```json"):
                # Extract content between ```json and ```
                start_idx = response_content_clean.find("```json") + 7
                end_idx = response_content_clean.find("```", start_idx)
                if end_idx != -1:
                    response_content_clean = response_content_clean[start_idx:end_idx].strip()
            elif response_content_clean.startswith("```"):
                # Extract content between ``` and ```
                start_idx = response_content_clean.find("```") + 3
                end_idx = response_content_clean.find("```", start_idx)
                if end_idx != -1:
                    response_content_clean = response_content_clean[start_idx:end_idx].strip()
            
            # Try to find JSON object boundaries if response has extra text
            if not response_content_clean.startswith("{"):
                # Look for first { and last }
                start_brace = response_content_clean.find("{")
                end_brace = response_content_clean.rfind("}")
                if start_brace != -1 and end_brace != -1 and end_brace > start_brace:
                    response_content_clean = response_content_clean[start_brace:end_brace+1]
            
            ai_response = json.loads(response_content_clean)
            
            # Validate response structure
            required_fields = ["alignment_label", "technical_view", "fundamental_view", "overall_comment", "confidence"]
            missing_fields = [field for field in required_fields if field not in ai_response]
            
            if missing_fields:
                logger.warning(f"AI response missing fields: {missing_fields}. Using defaults.")
                for field in missing_fields:
                    if field == "confidence":
                        ai_response[field] = 0
                    else:
                        ai_response[field] = ""
            
            # Validate alignment_label
            valid_labels = ["Strongly Agree", "Agree", "Neutral", "Disagree", "Strongly Disagree"]
            if ai_response.get("alignment_label") not in valid_labels:
                logger.warning(f"Invalid alignment_label: {ai_response.get('alignment_label')}. Using 'Neutral'.")
                ai_response["alignment_label"] = "Neutral"
            
            # Validate confidence
            confidence = ai_response.get("confidence", 0)
            if not isinstance(confidence, (int, float)) or confidence < 0 or confidence > 100:
                logger.warning(f"Invalid confidence: {confidence}. Using 0.")
                ai_response["confidence"] = 0
            
            return ai_response
            
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing AI JSON response: {e}")
            logger.error(f"Response content: {response_content[:500]}")
            return {
                "error": f"JSON parse error: {e}",
                "alignment_label": "AI Parse Error",
                "technical_view": "",
                "fundamental_view": "",
                "overall_comment": "",
                "confidence": 0
            }
            
    except openai.APITimeoutError:
        logger.error(f"OpenAI API timeout after {timeout}s")
        return {
            "error": "API timeout",
            "alignment_label": "AI Timeout",
            "technical_view": "",
            "fundamental_view": "",
            "overall_comment": "",
            "confidence": 0
        }
    except openai.APIError as e:
        logger.error(f"OpenAI API error: {e}")
        return {
            "error": f"API error: {e}",
            "alignment_label": "AI Error",
            "technical_view": "",
            "fundamental_view": "",
            "overall_comment": "",
            "confidence": 0
        }
    except Exception as e:
        logger.error(f"Unexpected error calling OpenAI API: {e}", exc_info=True)
        return {
            "error": f"Unexpected error: {e}",
            "alignment_label": "AI Error",
            "technical_view": "",
            "fundamental_view": "",
            "overall_comment": "",
            "confidence": 0
        }


def _get_ai_alignment_multipass(
    trade_payload: Dict,
    api_key: Optional[str],
    model: str,
    temperature: float,
    max_tokens: int,
    timeout: int,
    num_passes: int
) -> Dict:
    """
    Multi-pass AI analysis for improved accuracy.
    
    Pass 1: Initial analysis
    Pass 2: Review and refine (feed Pass 1 back)
    Pass 3: Final validation (feed Pass 2 back, ensure consistency)
    
    Args:
        trade_payload: Trade payload dictionary
        api_key: OpenAI API key
        model: Model name
        temperature: Temperature setting
        max_tokens: Max tokens per pass
        timeout: Timeout per pass
        num_passes: Number of passes (default 3)
    
    Returns:
        Final AI response dictionary
    """
    logger.info(f"Starting multi-pass AI analysis ({num_passes} passes)...")
    
    # Pass 1: Initial analysis
    logger.info("  [Pass 1/{}] Initial analysis...".format(num_passes))
    pass1_response = _get_ai_alignment_single_pass(
        trade_payload, None, api_key, model, temperature, max_tokens, timeout, pass_number=1
    )
    
    if "error" in pass1_response:
        logger.warning("  Pass 1 failed, returning error response")
        return pass1_response
    
    # Pass 2: Review and refine (if num_passes >= 2)
    if num_passes >= 2:
        logger.info("  [Pass 2/{}] Review and refine...".format(num_passes))
        pass2_response = _get_ai_alignment_single_pass(
            trade_payload, pass1_response, api_key, model, temperature, max_tokens, timeout, pass_number=2
        )
        
        if "error" in pass2_response:
            logger.warning("  Pass 2 failed, using Pass 1 response")
            return pass1_response
        
        # Pass 3: Final validation (if num_passes >= 3)
        if num_passes >= 3:
            logger.info("  [Pass 3/{}] Final validation...".format(num_passes))
            pass3_response = _get_ai_alignment_single_pass(
                trade_payload, pass2_response, api_key, model, temperature, max_tokens, timeout, pass_number=3
            )
            
            if "error" in pass3_response:
                logger.warning("  Pass 3 failed, using Pass 2 response")
                return pass2_response
            
            logger.info("  ✓ Multi-pass analysis complete (using Pass 3)")
            return pass3_response
        else:
            logger.info("  ✓ Multi-pass analysis complete (using Pass 2)")
            return pass2_response
    else:
        logger.info("  ✓ Single-pass analysis complete")
        return pass1_response


def _get_ai_alignment_single_pass(
    trade_payload: Dict,
    previous_response: Optional[Dict],
    api_key: Optional[str],
    model: str,
    temperature: float,
    max_tokens: int,
    timeout: int,
    pass_number: int = 1
) -> Dict:
    """
    Single pass of AI analysis (used by multi-pass system).
    
    Args:
        trade_payload: Trade payload dictionary
        previous_response: Previous pass response (None for Pass 1)
        api_key: OpenAI API key
        model: Model name
        temperature: Temperature setting
        max_tokens: Max tokens
        timeout: Timeout
        pass_number: Pass number (1, 2, or 3)
    
    Returns:
        AI response dictionary
    """
    # Initialize OpenAI client
    try:
        client = openai.OpenAI(api_key=api_key)
    except Exception as e:
        logger.error(f"  Pass {pass_number} - Error initializing OpenAI client: {e}")
        return {
            "error": f"OpenAI client initialization failed: {e}",
            "alignment_label": "AI Error",
            "technical_view": "",
            "fundamental_view": "",
            "overall_comment": "",
            "confidence": 0
        }
    
    # Build system message
    system_message = """You are a senior energy commodity analyst with deep expertise in:
- Energy commodity markets (crude oil, refined products, NGLs, natural gas)
- Technical analysis and trading signals
- Spread trading (inter-commodity, inter-tenor, calendar spreads)
- Fundamental analysis of supply/demand, seasonality, and market structure
- Historical market conditions and price dynamics

Your task is to RESEARCH and BALANCED evaluate trade signals as a true analyst would.

BALANCED ANALYST APPROACH:
1. RESEARCH THE MARKET: Use your knowledge of energy markets to understand what was happening at the trade date/timeframe
   - Consider: seasonal patterns, supply/demand fundamentals, geopolitical events, inventory levels, refinery runs
   - Think about: What were typical spreads at that time? What were seasonal expectations?
   - Analyze: Are the commodities in the trade fundamentally aligned or misaligned for that timeframe?
   - Use forward curve data to assess market structure (contango/backwardation)

2. BE BALANCED AND HONEST:
   - Use the FULL RANGE of alignment labels appropriately: Strongly Agree, Agree, Neutral, Disagree, Strongly Disagree
   - AGREE when: Technical indicators are strong (high score, multiple confirmations) AND fundamentals/market structure support the trade
   - DISAGREE when: There are clear fundamental mismatches, poor risk/reward, or market structure contradicts the trade
   - Don't be overly negative - recognize when a trade has merit despite some concerns
   - A trade with strong technicals (score 95+) and reasonable fundamentals should often be "Agree" or "Neutral", not automatically "Disagree"

3. EVALUATION CRITERIA:
   - Strong technicals (score 95-110) + supportive fundamentals = Agree/Strongly Agree
   - Strong technicals + neutral/mixed fundamentals = Neutral/Agree
   - Strong technicals + clearly negative fundamentals = Disagree
   - Weak technicals OR poor risk/reward = Disagree/Strongly Disagree
   - Give credit where due - if technicals are excellent and fundamentals are reasonable, don't default to Disagree

CRITICAL UNDERSTANDING OF SPREADS:
- Spreads have two legs: Leg 1 and Leg 2
- A "Buy" spread means: Buy Leg 1 / Sell Leg 2
- A "Sell" spread means: Sell Leg 1 / Buy Leg 2
- Legs can involve different commodities and different tenors (including quarterlies like Q1, Q2, Q3, Q4)
- Outrights have only one leg (Leg 1), and the signal direction applies directly to it

You must evaluate each trade from both:
1. Technical perspective: Based on the score details, indicators, and risk metrics provided
2. Fundamental perspective: Based on your RESEARCH of market conditions, commodities, tenors, and seasonality
3. Market structure perspective: Analyze the forward curves provided
   - CONTANGO (prices rising over time): BEARISH structure - indicates oversupply/storage costs, bearish for longs
   - BACKWARDATION (prices falling over time): BULLISH structure - indicates tight supply/demand, bullish for longs
   - For BUY signals: Backwardation is supportive, Contango is bearish/negative
   - For SELL signals: Contango is supportive, Backwardation is bullish/negative
   - Consider if the trade direction aligns with the forward curve structure

Provide your assessment as JSON only, with no additional text."""

    # Build user message based on pass number
    if pass_number == 1:
        # Pass 1: Initial analysis
        user_message = _build_user_message(trade_payload)
    elif pass_number == 2:
        # Pass 2: Review and refine
        user_message = _build_user_message(trade_payload)
        user_message += f"\n\n--- PREVIOUS ANALYSIS (PASS 1) ---\n"
        user_message += f"Alignment: {previous_response.get('alignment_label', 'N/A')}\n"
        user_message += f"Technical View: {previous_response.get('technical_view', 'N/A')}\n"
        user_message += f"Fundamental View: {previous_response.get('fundamental_view', 'N/A')}\n"
        user_message += f"Overall Comment: {previous_response.get('overall_comment', 'N/A')}\n"
        user_message += f"Confidence: {previous_response.get('confidence', 0)}\n"
        user_message += f"\n--- YOUR TASK (PASS 2) ---\n"
        user_message += "Review the previous analysis above. RESEARCH the market conditions for this timeframe and provide a BALANCED, IMPROVED analysis.\n"
        user_message += "- RESEARCH: Consider what was happening in energy markets at this date - seasonality, fundamentals, typical spreads, forward curve structure\n"
        user_message += "- Be BALANCED: If technicals are strong (score 95+) and fundamentals are reasonable, consider Agree. Only use Disagree if there are CLEAR fundamental contradictions\n"
        user_message += "- Don't be overly negative - recognize when strong technicals deserve credit even if fundamentals are mixed\n"
        user_message += "- Keep technical_view, fundamental_view, and overall_comment to ONE LINE each (max 80 characters)\n"
        user_message += "- Ensure your alignment label accurately reflects your MARKET RESEARCH and analysis (use full range: Strongly Agree to Strongly Disagree)\n"
        user_message += "- Increase confidence if you're more certain, decrease if less certain\n"
        user_message += "Respond with a complete, refined JSON response with SHORT, CONCISE bullet points."
    elif pass_number == 3:
        # Pass 3: Final validation
        user_message = _build_user_message(trade_payload)
        user_message += f"\n\n--- REFINED ANALYSIS (PASS 2) ---\n"
        user_message += f"Alignment: {previous_response.get('alignment_label', 'N/A')}\n"
        user_message += f"Technical View: {previous_response.get('technical_view', 'N/A')}\n"
        user_message += f"Fundamental View: {previous_response.get('fundamental_view', 'N/A')}\n"
        user_message += f"Overall Comment: {previous_response.get('overall_comment', 'N/A')}\n"
        user_message += f"Confidence: {previous_response.get('confidence', 0)}\n"
        user_message += f"\n--- YOUR TASK (PASS 3 - FINAL VALIDATION) ---\n"
        user_message += "Validate the refined analysis above. Ensure:\n"
        user_message += "- You've RESEARCHED the market conditions for this timeframe and your analysis reflects that research\n"
        user_message += "- The alignment label (Strongly Agree/Agree/Neutral/Disagree/Strongly Disagree) matches your MARKET RESEARCH and analysis\n"
        user_message += "- You're being BALANCED: If technicals are strong (score 95+) and fundamentals are reasonable/supportive, Agree is appropriate. Only use Disagree if there are CLEAR fundamental contradictions\n"
        user_message += "- Technical and fundamental views are consistent with each other (keep to ONE LINE each, max 80 chars)\n"
        user_message += "- Overall comment accurately summarizes both views and reflects your market research (ONE LINE, max 80 chars)\n"
        user_message += "- Confidence level is appropriate (0-100) based on how certain you are after market research\n"
        user_message += "If everything is consistent with your market research, return the same analysis. If not, provide corrections.\n"
        user_message += "Respond with a complete, validated JSON response with SHORT, CONCISE bullet points."
    else:
        # Fallback: use Pass 1 format
        user_message = _build_user_message(trade_payload)
    
    # Make API call
    start_time = time.time()
    try:
        # Note: response_format={"type": "json_object"} only works with certain models (gpt-4-turbo, gpt-4o)
        # For gpt-4, we'll request JSON in the prompt and parse it manually
        use_json_format = model in ["gpt-4-turbo", "gpt-4-turbo-preview", "gpt-4o", "gpt-4o-mini"]
        
        # Enhance user message to strongly request JSON if not using response_format
        if not use_json_format:
            user_message += "\n\nIMPORTANT: Respond ONLY with valid JSON. Do not include any text before or after the JSON object."
        
        api_params = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_message},
                {"role": "user", "content": user_message}
            ],
            "temperature": temperature,
            "max_tokens": max_tokens
        }
        
        if use_json_format:
            api_params["response_format"] = {"type": "json_object"}
        
        response = client.chat.completions.create(**api_params)
        
        duration = time.time() - start_time
        logger.info(f"  Pass {pass_number} completed in {duration:.2f}s")
        
        # Extract response content
        response_content = response.choices[0].message.content
        
        # Parse JSON response - handle cases where response might have extra text
        try:
            # Try to extract JSON from response (in case model adds extra text)
            response_content_clean = response_content.strip()
            
            # If response starts with ```json or ```, extract JSON part
            if response_content_clean.startswith("```json"):
                # Extract content between ```json and ```
                start_idx = response_content_clean.find("```json") + 7
                end_idx = response_content_clean.find("```", start_idx)
                if end_idx != -1:
                    response_content_clean = response_content_clean[start_idx:end_idx].strip()
            elif response_content_clean.startswith("```"):
                # Extract content between ``` and ```
                start_idx = response_content_clean.find("```") + 3
                end_idx = response_content_clean.find("```", start_idx)
                if end_idx != -1:
                    response_content_clean = response_content_clean[start_idx:end_idx].strip()
            
            # Try to find JSON object boundaries if response has extra text
            if not response_content_clean.startswith("{"):
                # Look for first { and last }
                start_brace = response_content_clean.find("{")
                end_brace = response_content_clean.rfind("}")
                if start_brace != -1 and end_brace != -1 and end_brace > start_brace:
                    response_content_clean = response_content_clean[start_brace:end_brace+1]
            
            ai_response = json.loads(response_content_clean)
            
            # Validate response structure
            required_fields = ["alignment_label", "technical_view", "fundamental_view", "overall_comment", "confidence"]
            missing_fields = [field for field in required_fields if field not in ai_response]
            
            if missing_fields:
                logger.warning(f"  Pass {pass_number} response missing fields: {missing_fields}. Using defaults.")
                for field in missing_fields:
                    if field == "confidence":
                        ai_response[field] = 0
                    else:
                        ai_response[field] = ""
            
            # Validate alignment_label
            valid_labels = ["Strongly Agree", "Agree", "Neutral", "Disagree", "Strongly Disagree"]
            if ai_response.get("alignment_label") not in valid_labels:
                logger.warning(f"  Pass {pass_number} invalid alignment_label: {ai_response.get('alignment_label')}. Using 'Neutral'.")
                ai_response["alignment_label"] = "Neutral"
            
            # Validate confidence
            confidence = ai_response.get("confidence", 0)
            if not isinstance(confidence, (int, float)) or confidence < 0 or confidence > 100:
                logger.warning(f"  Pass {pass_number} invalid confidence: {confidence}. Using 0.")
                ai_response["confidence"] = 0
            
            return ai_response
            
        except json.JSONDecodeError as e:
            logger.error(f"  Pass {pass_number} JSON parse error: {e}")
            logger.error(f"  Response content: {response_content[:500]}")
            return {
                "error": f"JSON parse error: {e}",
                "alignment_label": "AI Parse Error",
                "technical_view": "",
                "fundamental_view": "",
                "overall_comment": "",
                "confidence": 0
            }
            
    except openai.APITimeoutError:
        logger.error(f"  Pass {pass_number} timeout after {timeout}s")
        return {
            "error": "API timeout",
            "alignment_label": "AI Timeout",
            "technical_view": "",
            "fundamental_view": "",
            "overall_comment": "",
            "confidence": 0
        }
    except openai.APIError as e:
        logger.error(f"  Pass {pass_number} API error: {e}")
        return {
            "error": f"API error: {e}",
            "alignment_label": "AI Error",
            "technical_view": "",
            "fundamental_view": "",
            "overall_comment": "",
            "confidence": 0
        }
    except Exception as e:
        logger.error(f"  Pass {pass_number} unexpected error: {e}", exc_info=True)
        return {
            "error": f"Unexpected error: {e}",
            "alignment_label": "AI Error",
            "technical_view": "",
            "fundamental_view": "",
            "overall_comment": "",
            "confidence": 0
        }


def _build_user_message(trade_payload: Dict) -> str:
    """
    Build user message for OpenAI API from trade_payload.
    
    Args:
        trade_payload: Structured trade payload dictionary
    
    Returns:
        Formatted user message string
    """
    structure_type = trade_payload.get("structure_type", "unknown")
    spread_expression = trade_payload.get("spread_expression", "")
    strategy_type = trade_payload.get("strategy_type", "Unknown")
    signal_direction = trade_payload.get("signal_direction", "Unknown")
    legs = trade_payload.get("legs", [])
    
    # Build structure description
    if structure_type == "spread":
        structure_desc = f"SPREAD ({spread_expression})"
    else:
        structure_desc = "OUTRIGHT"
    
    # Build legs description
    legs_desc = []
    for leg in legs:
        leg_index = leg.get("leg_index", 0)
        leg_role = leg.get("leg_role", "")
        commodity = leg.get("commodity", "")
        location = leg.get("location", "")
        tenor = leg.get("tenor", "")
        symbol = leg.get("symbol", "")
        volume = leg.get("volume", "")
        is_quarterly = leg.get("is_quarterly", False)
        
        leg_str = f"Leg {leg_index} ({leg_role}): {commodity}"
        if location:
            leg_str += f" ({location})"
        leg_str += f" in {tenor}"
        leg_str += f"\n  - Symbol: {symbol}"
        if volume:
            leg_str += f"\n  - Volume: {volume}"
        if is_quarterly:
            leg_str += f"\n  - Quarterly contract"
        
        legs_desc.append(leg_str)
    
    # Build direction interpretation
    if structure_type == "spread":
        if signal_direction == "Buy":
            direction_interp = "This is a Buy spread: Buy Leg 1 / Sell Leg 2"
        else:
            direction_interp = "This is a Sell spread: Sell Leg 1 / Buy Leg 2"
    else:
        if signal_direction == "Buy":
            direction_interp = "This is a Buy outright: Buy Leg 1"
        else:
            direction_interp = "This is a Sell outright: Sell Leg 1"
    
    # Build pricing section
    entry_price_label = trade_payload.get("entry_price_label", "")
    entry_price_numeric = trade_payload.get("entry_price_numeric", 0)
    stop_price_label = trade_payload.get("stop_price_label", "")
    stop_price_numeric = trade_payload.get("stop_price_numeric", 0)
    target_price_label = trade_payload.get("target_price_label", "")
    target_price_numeric = trade_payload.get("target_price_numeric", 0)
    position_pct = trade_payload.get("position_pct", 0)
    signal_score = trade_payload.get("signal_score", 0)
    
    # Build risk metrics
    risk_metrics = trade_payload.get("risk_metrics", {})
    atr = risk_metrics.get("atr", 0)
    stop_multiple = risk_metrics.get("stop_multiple", 0)
    target_multiple = risk_metrics.get("target_multiple", 0)
    
    # Build full message
    week_date = trade_payload.get("week_date", "")
    message = f"""Evaluate this trade signal:

TRADE DATE (RESEARCH THIS TIMEFRAME): {week_date}
IMPORTANT: Use your knowledge of energy markets to understand what was happening at this date/timeframe.

STRUCTURE: {structure_desc}

STRATEGY: {strategy_type}
DIRECTION: {signal_direction}

LEGS:
{chr(10).join(legs_desc)}

DIRECTION INTERPRETATION:
{direction_interp}

PRICING:
- Entry: {entry_price_label} ({entry_price_numeric})
- Stop: {stop_price_label} ({stop_price_numeric})
- Target: {target_price_label} ({target_price_numeric})
- Position Size: {position_pct}%

SIGNAL SCORE: {signal_score}

SCORE DETAILS:
{trade_payload.get("score_details_raw", "")}

RISK METRICS:
{trade_payload.get("risk_details_raw", "")}

ICE CHAT CONTEXT:
{trade_payload.get("ice_chat_raw", "")}

FORWARD CURVE DATA (MARKET STRUCTURE):
{_format_forward_curves(trade_payload.get("forward_curves", {}))}

EVALUATION QUESTIONS (RESEARCH THE MARKET FOR DATE: {week_date}):
1. TECHNICAL ASSESSMENT: Evaluate the technical indicators, score, and risk metrics. Are they strong? What are the strengths and any weaknesses? (ONE LINE ONLY - max 80 characters)
2. FUNDAMENTAL ASSESSMENT: RESEARCH the market conditions for this timeframe. Consider seasonality, supply/demand, forward curve structure, and commodity relationships. Do fundamentals support, contradict, or are they neutral for this trade? (ONE LINE ONLY - max 80 characters)
3. OVERALL ALIGNMENT: Based on your MARKET RESEARCH, balance technical strength with fundamental support. If technicals are strong (score 95+) and fundamentals are reasonable/supportive, consider Agree. Only use Disagree if there are CLEAR fundamental contradictions. (ONE LINE ONLY - max 80 characters)

ALIGNMENT GUIDELINES (USE APPROPRIATELY):
- "Strongly Agree": Excellent setup - score 100+ with multiple technical confirmations AND strong fundamental/market structure support
- "Agree": Good setup - score 95+ with solid technicals AND fundamentals that support (or at least don't contradict) the trade
- "Neutral": Mixed signals - strong technicals but uncertain fundamentals, OR moderate technicals with reasonable fundamentals
- "Disagree": Significant concerns - either weak technicals (despite high score, missing key indicators) OR clear fundamental/market structure contradictions
- "Strongly Disagree": Major red flags - weak technicals AND fundamental misalignment, OR very poor risk/reward

IMPORTANT: If a trade has a score of 100+ with multiple technical confirmations (ADX, COINT, MACD, EMA alignment), it likely has merit. Only disagree if there are CLEAR fundamental or market structure issues that outweigh the technical strength.

IMPORTANT: Keep all responses SHORT and CONCISE:
- technical_view: ONE sentence, max 80 characters
- fundamental_view: ONE sentence, max 80 characters  
- overall_comment: ONE sentence, max 80 characters

Respond ONLY with valid JSON matching this schema:
{{
  "alignment_label": "Strongly Agree" | "Agree" | "Neutral" | "Disagree" | "Strongly Disagree",
  "technical_view": "...",
  "fundamental_view": "...",
  "overall_comment": "...",
  "confidence": 0-100
}}"""
    
    return message


def _format_forward_curves(forward_curves: Dict) -> str:
    """
    Format forward curve data for AI analysis.
    
    Args:
        forward_curves: Dictionary of forward curve data by root code
        
    Returns:
        Formatted string showing forward curves
    """
    if not forward_curves:
        return "Forward curve data not available."
    
    formatted = []
    for root_code, curve_info in forward_curves.items():
        commodity = curve_info.get("commodity", root_code)
        prices = curve_info.get("prices", {})
        
        if not prices:
            continue
        
        # Sort months chronologically
        sorted_months = sorted(prices.keys(), key=lambda x: (
            int(x.split('_')[1]) if '_' in x else 999,  # Year
            ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'].index(x.split('_')[0]) if '_' in x and x.split('_')[0] in ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'] else 999
        ))
        
        # Format as: Commodity: Jan_25=$X.XX, Feb_25=$Y.YY, ...
        price_strs = [f"{month}=${price:.2f}" for month, price in [(m, prices[m]) for m in sorted_months[:12]]]  # Limit to 12 months
        formatted.append(f"{commodity} ({root_code}): {', '.join(price_strs)}")
    
    if not formatted:
        return "Forward curve data not available."
    
    return "\n".join(formatted) + "\n\nMARKET STRUCTURE ANALYSIS:\n- CONTANGO (prices rising over time) = BEARISH structure (oversupply/storage costs) - bearish for BUY signals, supportive for SELL signals\n- BACKWARDATION (prices falling over time) = BULLISH structure (tight supply/demand) - bullish for BUY signals, bearish for SELL signals\nEvaluate if the trade direction aligns with the forward curve structure."

