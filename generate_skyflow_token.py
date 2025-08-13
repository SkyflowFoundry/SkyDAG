#!/usr/bin/env python3
"""
Generate Skyflow JWT token from service account credentials
"""

import json
import jwt
import time
import requests
from datetime import datetime, timedelta


def generate_jwt_token(service_account_json_str):
    """Generate JWT token from Skyflow service account"""
    try:
        sa_data = json.loads(service_account_json_str)
    except json.JSONDecodeError:
        raise ValueError("Invalid service account JSON")
    
    # Create JWT payload
    now = datetime.utcnow()
    payload = {
        'iss': sa_data['clientID'],
        'key': sa_data['keyID'],
        'aud': sa_data['tokenURI'],
        'exp': int((now + timedelta(hours=1)).timestamp()),
        'sub': sa_data['clientID']
    }
    
    # Sign the JWT
    token = jwt.encode(
        payload,
        sa_data['privateKey'],
        algorithm='RS256',
        headers={'kid': sa_data['keyID']}
    )
    
    return token


def get_access_token(service_account_json_str):
    """Get access token from Skyflow"""
    jwt_token = generate_jwt_token(service_account_json_str)
    
    sa_data = json.loads(service_account_json_str)
    token_url = sa_data['tokenURI']
    
    response = requests.post(
        token_url,
        headers={
            'Content-Type': 'application/x-www-form-urlencoded'
        },
        data={
            'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            'assertion': jwt_token
        }
    )
    
    if response.status_code == 200:
        token_data = response.json()
        return token_data.get('access_token')
    else:
        raise Exception(f"Token request failed: {response.status_code} - {response.text}")


if __name__ == "__main__":
    # Load service account from your .env.local
    try:
        from dotenv import load_dotenv
        import os
        
        load_dotenv('.env.local')
        
        sa_json = os.getenv('SKYFLOW_DETECT_SERVICE_ACCOUNT')
        if not sa_json:
            print("❌ SKYFLOW_DETECT_SERVICE_ACCOUNT not found in .env.local")
            exit(1)
        
        print("🔑 Generating Skyflow access token...")
        access_token = get_access_token(sa_json)
        
        print("✅ Success! Your Skyflow Bearer token:")
        print(f"Bearer {access_token}")
        print()
        print("📋 Copy this value to your .env.local.working file:")
        print(f"SKYFLOW_AUTH_HEADER=Bearer {access_token}")
        
    except ImportError:
        print("❌ Please install required packages:")
        print("pip install python-dotenv pyjwt[crypto] requests")
    except Exception as e:
        print(f"❌ Error generating token: {e}")