"""
The workload manager class is reponsible for allowing
the distribution of requests among various workers.
"""

import logging
import json
import threading
import random
import os
from datetime import datetime

from src import skald


class WorkloadManager:
    """
    The Workload Manager is responsible for receiving a workload
    and forward it to a Skald instance in an efficient way, per-
    forming the necessary operations to ensure compatibility.

    Attributes
    ----------
    consolidator : Skald
        consolidation instance

    Methods
    -------
    run(payload):
        Executes the consolidation process for the given payload.
    """

    def __init__(self):
        logging.info("%s - Workload Manager: initializing", threading.get_ident())
        # Set variables
        self.stateful = os.environ.get("STATEFUL", "false").lower() == "true"
        # Initialize consolidator
        self.consolidator = skald.Skald(
            k=int(os.environ.get("K", 10)),
            lf=float(os.environ.get("LF", 1)),
            dampening=float(os.environ.get("DAMPENING", 0.1)),
            influence=float(os.environ.get("INFLUENCE", 0.8)),
            stateful=os.environ.get("STATEFUL", "false").lower() == "true",
        )

    def convert_claims(self, object: str, datatype: str, claims: list):
        """
        Receives the object name and list of claims, and converts
        to the format required by the consolidation module.

            Parameters:
                object (str): Name of the object
                datatype (str): Datatype of the object
                claims (list): List containing claims

            Returns:
                list: Object in valid format
        """
        # Initialize the list
        converted = []
        # Iterate the claims
        for claim in claims:
            if datatype == "address":
                # Iterate address fields
                for field in claim.fact:
                    if field[1] is not None:
                        # Initialize the data
                        data = {}
                        # Extract data
                        data["sourceId"] = claim.sourceId
                        data["object"] = object + "-" + str(field[0])
                        data["fact"] = str(field[1])
                        data["datatype"] = "string"
                        # Append to list
                        converted.append(data)
            elif datatype == "list-string":
                # Iterate the list elements
                for element in claim.fact:
                    # Initialize the data
                    data = {}
                    # Extract data
                    data["sourceId"] = claim.sourceId
                    data["object"] = object
                    data["fact"] = element
                    data["datatype"] = "string"
                    # Append to list
                    converted.append(data)
            elif datatype == "list-categorical":
                # Iterate the list elements
                for element in claim.fact:
                    # Initialize the data
                    data = {}
                    # Extract data
                    data["sourceId"] = claim.sourceId
                    data["object"] = object
                    data["fact"] = element
                    data["datatype"] = "categorical"
                    # Append to list
                    converted.append(data)
            else:
                # Initialize the data
                data = {}
                # Extract data
                data["sourceId"] = claim.sourceId
                data["object"] = object
                data["fact"] = claim.fact
                data["datatype"] = datatype
                # Append to list
                converted.append(data)
        # Return list
        return converted

    def convert_sources(self, sources: list):
        """
        Receives the list of sources of claims, and converts
        to the format required by the consolidation module.

            Parameters:
                sources (list): List containing sources

            Returns:
                list: Object in valid format
        """
        # Initialize the list
        converted = []
        # Iterate the claims
        for source in sources:
            # Initialize the data
            data = {}
            # Extract data
            data["sourceId"] = source.sourceId
            data["reputation"] = source.reputation
            data["probabilities"] = source.probabilities
            data["ratings"] = source.ratings
            # Append to list
            converted.append(data)
        # Return list
        return converted

    def clear_reputation(self):
        """
        This function has the sole purpose of deleting all
        the information regarding reputation for all sources.

        Returns:
                success: 1 for success, and -1 otherwise
        """
        try:
            # Clear the reputation
            self.consolidator.clear_reputation()
            # Return success
            return 1
        except Exception as e:
            # Log error
            logging.error("Error deleting reputation data: %s", e)
            # Return error
            return -1

    def run(self, objects, sources):
        """
        Receives a payload and runs the consolidation process for it.

            Parameters:
                objects (list): List containing objects and claims
                sources (list): List containing sources

            Returns:
                json: Result of the consolidation process
        """
        try:
            logging.info(
                "%s - Workload Manager: executing workload", threading.get_ident()
            )
            # Get the current datetime
            current_date = datetime.now()
            # Initialize structure
            response = {
                "timestamp": current_date.isoformat(),
                "objects": [],
                "sources": [],
            }
            # Shuffle the order of the object list
            random.shuffle(objects)
            # Check if mode is stateless
            if self.stateful is False:
                # Convert sources to the required format
                sources = self.convert_sources(sources)
            # Iterate the various objects in the request
            for obj in objects:
                # Convert request to the required format
                claims = self.convert_claims(obj.name, obj.datatype, obj.claims)
                # Execute the consolidation process
                result, updated_sources = self.consolidator.consolidate(claims, sources)
                # Add result to response
                response["objects"].append(result)
                # Add sources to response
                for source in updated_sources:
                    # Check if source is already in list
                    for s in response["sources"]:
                        # Check if the source is already in list
                        if s["sourceId"] == source["sourceId"]:
                            # Remove element
                            response["sources"].remove(s)
                    # Append the source
                    response["sources"].append(source)
            logging.info(
                "%s - Workload Manager: workload executed", threading.get_ident()
            )
            try:
                json.dumps(response)
            except ValueError as e:
                logging.error("JSON serialization failed: %s", e)
                logging.error("Offending response: %s", response)
                raise
            # Return the result
            return response
        # Catch any exception
        except Exception as e:
            # Raise the exception
            raise e
