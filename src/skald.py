"""
This module contains the class of the Skald framework. It 
defines the available interface and connects the various 
modules in the underlying model.
"""

import logging
import threading
import pandas as pd
from fastapi import HTTPException

from src.consolidation import truthfinderV2
from src.reputation import reputation

class Skald:
    """
    The Skald class contains the logic required to receive 
    affirmations from sources, and output the list of 
    confidence scores of each affirmation.

    Attributes
    ----------
    k : integer
        number of levels in reputation system
    lf : float
        longevity factor for reputation
    dampening : float
        dampening factor for consolidation
    influence : float
        influence between related facts in consolidation
    stateful: bool
        defines wether the service is stateful or stateless

    Methods
    -------
    consolidate(dataframe):
        Executes the consolidation process.
    """

    def __init__(self, k, lf=1, dampening=0.5, influence=0.5, stateful=False):
        # Log execution
        logging.info('%s - Initializing framework', threading.get_ident())
        try:
            # Save supplied parameters
            self.k = k
            self.lf = lf
            self.dampening = dampening
            self.influence = influence
            self.stateful = stateful
            try:
                # Log execution
                logging.info('%s - Initializing consolidation module',
                             threading.get_ident())
                # Initialize the consolidation algorithm
                self.consolidator = truthfinderV2.TruthFinder(
                    dampening_factor=self.dampening,
                    influence_related=self.influence
                )
                # Log execution
                logging.info('%s - Consolidation module initialized',
                             threading.get_ident())
                try:
                    # Log execution
                    logging.info('%s - Initializing reputation module',
                                 threading.get_ident())
                    # Initialize the reputation algorithm
                    self.reputation = \
                        reputation.Reputation(
                            k=self.k, c=self.k, lf=self.lf,
                            stateful=self.stateful
                        )
                    # Log execution
                    logging.info('%s - Reputation module initialized',
                                 threading.get_ident())
                except Exception as e:
                    # Log error
                    logging.error('Error initializing \
                                  reputation module: %s', e)
            except Exception as e:
                # Log error
                logging.error('Error initializing consolidation module: %s', e)
            # Log execution
            logging.info('%s - Framework initialized', threading.get_ident())
        except Exception as e:
            # Log error
            logging.error('Error initializing framework: %s', e)

    def validate_input(self, input) -> bool:
        """
        Validates the input dataframe and returns the result as bool.

            Parameters:
                input (list): List containing affirmations

            Returns:
                bool: True if valid, False otherwise
        """
        # Define the valid fields
        fields = ('sourceId', 'object', 'fact')
        # Define the valid datatypes
        types = ('categorical', 'continuous', 'string')
        try:
            # Validate if input is a list
            if isinstance(input, list):
                # Iterate each element
                for affirmation in input:
                    # Validate affirmation format
                    if isinstance(affirmation, dict) and all(
                            k in affirmation for k in fields):
                        # Validate sourceId
                        if not isinstance(affirmation['sourceId'], str):
                            # Log error
                            logging.error('Invalid input format: \
                                          sourceId is invalid')
                            # Return False
                            return False
                        # Validate object
                        if not isinstance(affirmation['object'], str):
                            # Log error
                            logging.error('Invalid input format: \
                                          object is invalid')
                            # Return False
                            return False
                        # Validate datatype
                        #if not affirmation['datatype'] in types:
                            # Log error
                        #    logging.error('Invalid input format: \
                        #                  datatype is invalid')
                            # Return False
                        #    return False
                    else:
                        # Log error
                        logging.error('Invalid input format: \
                                      affirmation is invalid')
                        # Return False
                        return False
            else:
                # Log error
                logging.error('Invalid input format: input is not a list')
                # Return False
                return False
            # Log validation
            logging.info('%s - Input format passed validation',
                         threading.get_ident())
            # Return True
            return True
        except Exception as e:
            # Log error
            logging.error('Error validating input format: %s', e)

    def validate_sources(self, input, sources) -> bool:
        """
        Validates thesources and returns the result as bool.

            Parameters:
                input (list): List containing affirmations
                sources (list): List containing sources

            Returns:
                bool: True if valid, False otherwise
        """
        # Define the valid fields
        fields = ('sourceId', 'reputation', 'probabilities', 'ratings')
        # Extract sourceId's from input
        source_ids = [affirmation['sourceId'] for affirmation in input]
        try:
            # Validate if input is a list
            if isinstance(sources, list):
                # Iterate each element
                for source in sources:
                    # Validate affirmation format
                    if isinstance(source, dict) and all(
                            k in source for k in fields):
                        # Validate sourceId
                        if not isinstance(source['sourceId'], str):
                            # Log error
                            logging.error('Invalid source format: \
                                          sourceId is invalid')
                            # Return False
                            return False
                        # Validate reputation
                        if not isinstance(source['reputation'], float):
                            # Log error
                            logging.error('Invalid input format: \
                                          reputation score is invalid')
                            # Return False
                            return False
                        # Validate probabilities
                        if not isinstance(source['probabilities'], list):
                            # Log error
                            logging.error('Invalid input format: \
                                          probabilities are invalid')
                            # Return False
                            return False
                        # Validate ratings
                        if not isinstance(source['ratings'], list):
                            # Log error
                            logging.error('Invalid source format: \
                                          ratings are invalid')
                            # Return False
                            return False
                        # Validate lengths
                        if len(source['probabilities']) != self.k or \
                           len(source['ratings']) != self.k:
                            # Log error
                            logging.error('Invalid source format: \
                                          array size is invalid')
                            # Return False
                            return False
                    else:
                        # Log error
                        logging.error('Invalid source format: \
                                      affirmation is invalid')
                        # Return False
                        return False
                # Verify is all sources of claims are supplied
                for sid in source_ids:
                    if sid not in [source['sourceId'] for source in sources]:
                        # Log error
                        logging.error('Invalid source format: missing sources')
                        # Return False
                        return False
            else:
                # Log error
                logging.error('Invalid source format: source is not a list')
                # Return False
                return False
            # Log validation
            logging.info('%s - Source format passed validation',
                         threading.get_ident())
            # Return True
            return True
        except Exception as e:
            # Log error
            logging.error('Error validating source format: %s', e)

    def build_dataframe(self, input, sources):
        """
        This function receives a previously validated input, and builds
        a dataframe that is ready to be consumed by the consolidation
        module.

            Parameters:
                input (list): List containing the affirmations
                sources (list): List containing sources

            Returns:
                dataframe: dataframe containing original data
        """
        try:
            # Define columns
            cols = ['source', 'fact', 'object', 'datatype', 'trustworthiness']
            # Initialize structure for data
            data = []
            # Build a dictionary of sources indexed by sourceId if stateless
            source_dict = {s['sourceId']: s for s in sources} if not self.stateful else None
            # Iterate input
            for affirmation in input:
                source_id = affirmation['sourceId']
                # Verify if execution is stateful
                if self.stateful:
                    # Get the reputation for the source
                    rep = self.reputation.get_reputation(source_id)
                #if self.stateful is True:
                    # Get the reputation for the source
                    #rep = self.reputation.get_reputation(
                    #    affirmation['sourceId'])
                else:
                    # Lookup the source in stateless mode using the dictionary
                    rep = source_dict[source_id]['reputation']
                    # Get the entry for the source
                    # source = next(s for s in sources
                                #  if s['sourceId'] == affirmation['sourceId'])
                    # Get the reputation for the source
                    # rep = source['reputation']
                # Extract affirmation data
                entry = [
                    affirmation['sourceId'],
                    affirmation['fact'],
                    affirmation['object'],
                    affirmation['datatype'],
                    rep
                ]
                # Insert in data structure
                data.append(entry)
            # Create the dataframe from the collected data
            dataframe = pd.DataFrame(data, columns=cols)
            # Return the dataframe
            return dataframe
        except Exception as e:
            # Log error
            logging.error('Error building dataframe: %s', e)

    def build_response(self, dataframe):
        """
        This function receives a previously consolidated input, and builds
        a structure that can be returned with all information.

            Parameters:
                dataframe (dataframe): Result from consolidation

            Returns:
                object: response object containing fact and scores
        """
        try:
            if "address" in dataframe['object'].iloc[0]:
                # Initialize structure
                response = {
                    "name": "address",
                    "claims": [{
                        "fact": {},
                        "confidence": 0.0
                    }]
                }
                # Get the address fields present
                address_fields = dataframe['object'].unique()
                # Initialize the confidence
                confidence = 0.0
                # Initialize the field counter
                counter = 0
                # Loop over the unique values and create separate dataframes
                for obj in address_fields:
                    # Increment counter
                    counter += 1
                    # Get the entries for the object
                    df = dataframe[dataframe['object'] == obj]
                    # Remove the prefix from the object
                    obj = obj.removeprefix("address-")
                    # Sort dataframe by fact confidence
                    df = df.sort_values(by=['fact_confidence'], ascending=False)
                    # Put the fact with highest confidence in response
                    response['claims'][0]['fact'][obj] = df['fact'].iloc[0]
                    # Add confidence
                    confidence += df['fact_confidence'].iloc[0]
                # Calculate the confidence score (average confidence of fields)
                confidence = round(confidence / counter, 3)
                # Set the confidence
                response['claims'][0]['confidence'] = confidence
                return response
            else:
                # Initialize structure
                response = {
                    "name": dataframe['object'].iloc[0],
                    "claims": []
                }
                # Sort dataframe by fact confidence
                dataframe = dataframe.sort_values(
                    by=['fact_confidence'], ascending=False)
                # Add facts to structure
                for _, row in dataframe.iterrows():
                    response['claims'].append({
                        "fact": float(row['fact']) if row['datatype'] == 'continuous'\
                              else bool(row['fact']) if row['datatype'] == 'boolean'\
                              else row['fact'],
                        "confidence": round(row['fact_confidence'], 3),
                        "sourceId": row['source']
                    })
                # Return response
                return response
        except Exception as e:
            # Log error
            logging.error('Error building response: %s', e)
    
    def calculate_ratings(self, dataframe) -> list:
        """
        Thus function calculates the ratings for sources after the
        consolidation proces, in order to be used to recalculate
        updated reputation scores.

            Parameters:
                dataframe (dataframe): Dataframe from consolidation

            Returns:
                list: list containing the ratings for each source
        """
        try:
            # Initialize ratings structure
            ratings = []
            # Iterate through the dataframe
            for index, row in dataframe.iterrows():
                # Calculate rating
                rating = [0] * self.k
                # Get the index
                index = (self.k - 1) if row['fact_confidence'] == 1 \
                    else int(row['fact_confidence'] * self.k)
                # Set the rating in the respective index
                rating[index] = 1
                # Log result
                logging.info("%s - Source %s provided a fact with \
                             confidence %s resulting in rating %s",
                             threading.get_ident(),
                             row['source'], row['fact_confidence'], rating)
                # Build entry
                entry = {
                    "sourceId": row['source'],
                    "rating": rating
                }
                # Append to ratings
                ratings.append(entry)
            # Return ratings
            return ratings
        except Exception as e:
            # Log error
            logging.error('Error calculating ratings: %s', e)

    def get_sources(self, input):
        """
        This function retrieves the updated source entries for the sources.

            Parameters:
                input (list): List containing sources, facts, object,
                            and datatypes

            Returns:
                list: List containing the source entries
        """
        try:
            # Initialize structure
            sources = []
            # Iterate input
            for entry in input:
                # Extract source and retrieve from storafe
                source = self.reputation.get_source(entry['sourceId'])
                # Delete the identifier from the database
                del source["_id"]
                # Append to structure
                sources.append(source)
            # Return the dataframe
            return sources
        except Exception as e:
            # Log error
            logging.error('Error building dataframe: %s', e)

    def consolidate(self, claims, sources):
        """
        This functions consolidates the affirmations contained in the supplied
        dataframe, and returns a list of confidence scores for each fact.

            Parameters:
                claims (list): List containing sources, facts, object,
                            and datatypes
                sources (list): List containing sources and respective
                            reputation data

            Returns:
                json: json containing original data, as well as fact
                            confidence and source trustworthiness
                list: list of sources and their respective data after
                            the consolidation process
        """
        # Start execution
        logging.info('%s - Execution started', threading.get_ident())
        try:
            # Initialize structure
            updated_sources = []
            # Validate the input being supplied
            if self.validate_input(claims) is True:
                # Check if execution is stateless
                if self.stateful is False:
                    # Validate the sources
                    if self.validate_sources(claims, sources) is False:
                        # Raise invalid message
                        raise HTTPException(status_code=400,
                                            detail="Invalid source format")
                # Convert the input to a pandas dataframe
                dataframe = self.build_dataframe(claims, sources)
            else:
                # Raise invalid message
                raise HTTPException(status_code=400,
                                    detail="Invalid input format")
            # Execute the consolidation on the dataframe
            result = self.consolidator.run(dataframe, 1, 1e-4)
            # Calculate the ratings of each source
            ratings = self.calculate_ratings(result)
            # Iterate the ratings
            for rating in ratings:
                # Update reputation
                output = self.reputation.update_reputation(rating, sources)
                # Check if execution is stateless
                if self.stateful is False:
                    # Add updated sources to structure
                    updated_sources.append(output)
            # Build response
            response = self.build_response(result)
            # Check if execution is stateless
            if self.stateful is True:
                # Get the sources from the reputation storage
                updated_sources = self.get_sources(claims)
            # End execution
            logging.info('%s - Execution ended', threading.get_ident())
            # Return response
            return response, updated_sources
        except Exception as e:
            # Log the exception
            logging.error('An error occurred: %s', str(e))
            # Raise the exception
            raise e

    def clear_reputation(self):
        """
        This function has the sole purpose of deleting all
        the information regarding reputation for all sources.

        Returns:
                success: 1 for success, and -1 otherwise
        """
        try:
            # Clear the reputation
            self.reputation.clear_reputation()
            # Return success
            return 1
        except Exception as e:
            # Log error
            logging.error('Error deleting reputation data: %s', e)
            # Return error
            return -1
