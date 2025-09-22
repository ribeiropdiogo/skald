"""
This file contains the code for the API that servers as the
contact point with entities trying to consolkidate data.
"""

import threading
import logging
from datetime import datetime
import os
import uvicorn

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

import src.wm as WorkloadManager

# Load the configuration file
# with open("src/config.toml", "rb") as f:
#    config = tomllib.load(f)

# Instantiate the service
app = FastAPI()
# Define the logfile name
logfile = datetime.now().strftime("logs/%Y%m%d%H%M%S.log")
# Instantiate the logger
logging.basicConfig(
    filename=logfile,
    encoding="utf-8",
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class Source(BaseModel):
    """
    The Source class represents the format of a source entry
    for the reputation system.

    Attributes
    ----------
    sourceId : str
        identifier of the source of the claim
    reputation: float
        reputation score of the source
    probabilities: list
        probability scores for the source
    ratings: list
        aggregated ratings og the source
    """

    sourceId: str
    reputation: float
    probabilities: list[float]
    ratings: list[int]


class Address(BaseModel):
    """
    The Address class represents the format of an address.
    """

    street: str | None
    suburb: str | None
    province: str | None
    city: str | None
    district: str | None
    state: str | None
    postalCode: str | None
    country: str | None


class Claim(BaseModel):
    """
    The Claim class represents the format of consolidation
    claims made by sources.

    Attributes
    ----------
    sourceId : str
        identifier of the source of the claim
    fact: str | int | float | bool | list[str] | Address
        fact being supplied
    """

    sourceId: str
    fact: str | int | float | bool | list[str] | Address


class ConsolidatedClaim(BaseModel):
    """
    The ConsolidatedClaim class represents the format of
    consolidated claims.

    Attributes
    ----------
    fact: str | int | float
        supplied fact
    confidence: float
        confidence score of the claim
    sourceId: str
        identifier of the source
    """

    fact: float | int | str | bool
    confidence: float
    sourceId: str | None


class ConsolidatedAddress(BaseModel):
    """
    The ConsolidatedClaim class represents the format of
    consolidated address.

    Attributes
    ----------
    facts:
        list of fields
    confidence: float
        confidence score of the claim
    """

    fact: Address
    confidence: float


class Object(BaseModel):
    """
    The Object class represents the format of consolidation
    claims about an object.

    Attributes
    ----------
    name : str
        description of the object
    datatype: str
        dataype of the claim
    claims: list
        list of claims about the object
    """

    name: str
    datatype: str
    claims: list[Claim]


class ConsolidatedObject(BaseModel):
    """
    The ConsolidatedObject class represents the format of
    consolidated claims about an object.

    Attributes
    ----------
    name : str
        description of the object
    claims: list
        list of claims about the object
    """

    name: str
    claims: list[ConsolidatedClaim | ConsolidatedAddress]


class Request(BaseModel):
    """
    The Request class represents the format of consolidation
    requests made to the service.

    Attributes
    ----------
    objects : list
        list of objects to be consolidated
    sources : list
        list of sources supplying data
    """

    objects: list[Object]
    sources: list[Source] | None = None


class Response(BaseModel):
    """
    The Response class represents the format of consolidation
    response given to the service.

    Attributes
    ----------
    timestamp: str
        timestamp of request
    objects : list
        list of consolidated objects
    sources : list
        list of updated sources
    """

    timestamp: str
    objects: list[ConsolidatedObject]
    sources: list[Source]


def consolidate(request: Request):
    """
    Processes the payload of a request by handling the workload
    to a Workload Manager instance.

        Parameters:
            request (Request): The request containing the payload

        Returns:
            json: The result of the consolidation process
    """
    try:
        # Instantiate the workload manager
        wm = WorkloadManager.WorkloadManager()
        # Check if sources are suppleid for stateless execution
        if request.sources is None and os.environ.get("STATEFUL", "false") == "false":
            result = {"error": "stateless mode requires source information"}
        else:
            # Execute the workload
            result = wm.run(request.objects, request.sources)
        # Return the result
        return result
    # Catch any exeptions
    except Exception as e:
        # Raise any exceptions
        raise e


@app.get("/clear")
def clear():
    """
    Clears all reputation data.
    """
    logging.info("%s - Request received", threading.get_ident())
    # Pass the request to the consolidation function
    try:
        # Instantiate the workload manager
        wm = WorkloadManager.WorkloadManager()
        # Clear reputation
        result = wm.clear_reputation()
    except HTTPException as e:
        raise e
    # Return the result
    return result


@app.post("/consolidate")
def process(request: Request) -> Response:
    """
    Processes a consolidation request by passing the request to
    the consolidation function.

        Parameters:
            request (Request): The request containing the payload

        Returns:
            json: The result of the consolidation process
    """
    logging.info("%s - Request received", threading.get_ident())
    # Pass the request to the consolidation function
    try:
        result = consolidate(request)
    except HTTPException as e:
        raise e
    # Return the result
    return result


if __name__ == "__main__":
    # Lauch the server
    uvicorn.run(app, host="127.0.0.1", port=8000)
