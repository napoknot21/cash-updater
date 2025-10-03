from __future__ import annotations

import os
import requests
import base64
import msal
import jwt
import datetime as dt
import polars as pl

from typing import Dict, List, Optional, Any, Iterable, Union

from src.config import (
    APPLICATION_ID, SECRET_VALUE_ID, AUTHORITY, SCOPES, GRAPH_BASE,
    SHARED_MAILS, EMAIL_COLUMNS, 
    #MS, GS, SAXO, EDB, UBS,
    COUNTERPARTIES
)

def date_to_str (date : Optional[str | dt.datetime | dt.date] = None, format : str = "%Y-%m-%d") :
    
    if date is None :
        date = dt.datetime.now()

    if isinstance(date, str) :
        return str(date)

    return date.strftime(format)


def get_token (
        
        scopes : Optional[List] = None,
        app_id : Optional[str] =  None,
        authority : Optional[str] = None,
        secret :  Optional[str] = None
    
    ) -> Optional[str] :
    """
    Function get token from the applcation 
    """
    scopes = SCOPES if scopes is None else scopes

    app_id = APPLICATION_ID if app_id is None else app_id
    authority = AUTHORITY if authority is None else authority
    secret = SECRET_VALUE_ID if secret is None else secret
    
    app = msal.ConfidentialClientApplication(

        client_id=app_id,
        authority=authority,
        client_credential=secret

    )

    result = app.acquire_token_for_client(

        scopes=scopes

    )

        
    if "access_token" in result :

        print("\n[+] Token acquired successfully")
        print(result["access_token"][:30] + "...")  # Print just token first 30 letters
    
    else :

        print("\n[-] Failed to acquire token\n")
        print(result.get("error_description"))

    return result.get("access_token", None)


def decode_token (token : str) -> List[Dict[str, Any]] :

    if token is None :
        return None
    
    decoded = jwt.decode(token, options={"verify_signature": False})
    
    print("\n[*] Token claims :")
    print("\t[*] roles: ", decoded.get("roles"))
    print("\t[*] App Id:", decoded.get("appid"))
    
    return decoded


def get_inbox_messages_between (

        start_date : Optional[str | dt.datetime | dt.date] = None,
        end_date : Optional[str | dt.datetime | dt.date] = None,
        token : Optional[str] = None,
        email : Optional[str] = None,
        graph_base : Optional[str] = None,
        with_attach : bool = False

    ) :
    """
    
    """
    token = get_token() if token is None else token
    graph_base = GRAPH_BASE if graph_base is None else graph_base
    email = SHARED_MAILS[0] if email is None else email

    start_date = date_to_str(start_date)
    end_date = date_to_str(end_date)

    filter_str = f"receivedDateTime ge {start_date}"

    parameters = {
        
        "$orderby": "receivedDateTime ASC",
        "$select": "id,subject,from,receivedDateTime,hasAttachments",
        "$filter": filter_str,
        "$top": "100"

    }

    if with_attach is True :
        
        # Only metadata (id, name, contentType, size, isInline)
        parameters["$expand"] = "attachments($select=id,name,contentType,size,isInline)"


    headers = {
    
        "Authorization": f"Bearer {token}"
        
    }

    url = f"{graph_base}/users/{email}/mailFolders/Inbox/messages"

    rows: List[dict] = []
    while True :

        response = requests.get(
            
            url=url,
            headers=headers,
            params=parameters

        )

        if response.status_code != 200 :
            raise Exception(f"Graph API error {response.status_code}: {response.text}")
        
        data = response.json()

        for m in data.get("value", []) :

            rows.append(
            
                {
                    "id": m.get("id"),
                    "subject": m.get("subject"),
                    "from": m.get("from", {}).get("emailAddress", {}).get("address"),
                    "receivedDateTime": m.get("receivedDateTime"),
                    "hasAttachments": m.get("hasAttachments"),
                    "originEmail" : str(email)
                }
            
            )

        next_link = data.get("@odata.nextLink")

        if not next_link :
            print(f"Break here for {next_link}")
            break
        
        url = next_link
        params = None  # already encoded in nextLink

    df_email = pl.DataFrame(rows, schema_overrides=EMAIL_COLUMNS)

    return df_email


def list_message_attachments(token: str, email: str, message_id: str) -> List[Dict[str, Any]]:
    """
    Returns a list of attachments with metadata. For fileAttachment, Graph often includes contentBytes
    (base64) if the file isn't huge. Otherwise you can fetch raw bytes via $value.
    """
    headers = {"Authorization": f"Bearer {token}"}

    # Request relevant fields; contentBytes may be omitted for very large files
    params = {

        "$select" : "id,name,contentType,size,lastModifiedDateTime,contentBytes,@odata.type"

    }
    
    url = f"{GRAPH_BASE}/users/{email}/messages/{message_id}/attachments"
    
    r = requests.get(
        
        url,
        headers=headers,
        params=params
    
    )
    
    if r.status_code != 200 :
        raise Exception(f"Failed to list attachments: {r.status_code} - {r.text}")
    
    return r.json().get("value", [])


def save_message_attachements (
        
        token : str,
        email : str,
        message_id : str | int,
        dir_abs_path : str,
        subject_hint : Optional[str] = None
    
    ) :
    """
    
    """
    saved_paths : List[str] = []
    attachments = list_message_attachments(token, email, message_id)

    #msg_dir_name = safe_filename()

