"""
This module contains the class for the Reputation Module
and all the logic required for it to work and track
source reputations over time .
"""

import logging
import numpy as np

from pymongo import MongoClient

class Reputation:
    """
    The Reputation module class contains all the logic
    required to calculate source reputation upon receiving
    ratings and to track reputation over time.

    Attributes
    ----------
    k : integer
        number of levels in reputation system
    C : integer
        a-priori constant
    a : list
        default base rate
    lf : float
        longevity factor

    Methods
    -------
    get_reputation(sourceId):
        Returns the reputation for a given source.
    update_reputation(sourceId,reputation):
        Updates the reputation for a given source.
    """
    def __init__(self, k, c, lf, stateful):
        # Initialize MongoDB connector
        client = MongoClient(host='mongodb', port=27017, username='skald', password='skald')
        self.db = client.skald
        # instantiate variables
        self.k = k
        self.C = c
        self.a = np.full(k, 1 / (1*k))
        self.lf = lf
        self.pv = np.arange(k) / (k - 1)
        self.stateful = stateful

    def calculate_score(self, S, R) -> list:
        """
        This function calculates the multinomial probability
        reputation scores for a source using the supplied
        accumulated ratings.

            Parameters:
                S (list): Multinomial probability reputation scores
                R (ratings): Accumulated ratings

            Returns:
                S: Updated multinomial probability reputation scores
        """
        # Compute the accumulated ratings sum outside the loop (more efficient)
        acs = np.sum(R)
        # Compute the denominator outside the loop
        den = self.C + acs
        # Iterate through each level
        #for i in range(self.k):
            # Calculate the multinomial probability reputation score
        #    S[i] = (R[i] + (self.C * self.a[i])) / den
        S = (R + (self.C * self.a)) / den
        # Return S
        return S

    def point_estimate(self, S) -> float:
        """
        This function calculates the point estimate for a list
        of multinomial probability reputation scores.

            Parameters:
                S (list): Multinomial probability reputation scores

            Returns:
                reputation: Point estimate of the reputation score
        """
        # Initialize the point estimate
        pe = 0
        # Iterate through all levels (much slower)
        # for i in range(self.k):
            # Calculate the point value
            # pv = ((i + 1) - 1) / (self.k - 1)
            # Calculate the point estimate
            # pe += S[i] * pv
        pe = np.dot(S, self.pv)
        # Return the estimate
        return pe

    def update_reputation(self, rating, sources):
        """
        This functions is reponsible for receiving ratings for a source,
        and performing all the steps so that it's reputation is recalculated
        and then updated.

            Parameters:
                rating (json): Object containing the rating for a source
                sources (list): List of sources

            Returns:
                reputation: Point estimate of the reputation score
        """
        try:
            # Check if execution is stateless
            if self.stateful is True:
                # Get the source from the structure
                source = self.db.skald.find_one({'sourceId': rating['sourceId']})
            else:
                # Get the entry for the source
                source = next(s for s in sources
                              if s['sourceId'] == rating['sourceId'])
            # Extract the rating
            rating = np.array(rating['rating']) # convert to numpy for performance
            # Extract the accumulated ratings
            R = np.array(source['ratings']) # convert to numpy for performance
            # Extract the multinomial reputation scores
            S = np.array(source['probabilities']) # convert to numpy for performance
            # Check if the rating is valid
            if len(rating) != self.k:
                # The received rating is invalid
                raise Exception("Invalid rating format")
            else:
                # Apply aging on the accumulated ratings if lf  < 1
                if self.lf < 1:
                    #for i in range(self.k):
                    #    R[i] = self.lf * R[i]
                    R *= self.lf
                # Add the new rating
                # R = [sum(x) for x in zip(rating, R)] (much slower)
                # R = [r1 + r2 for r1, r2 in zip(rating, R)]
                R += rating
                # Calculate the multinomial probability reputation score
                S = self.calculate_score(S, R)
                # Point estimate after rating
                pe = self.point_estimate(S)
                # Check if execution is stateless
                if self.stateful is True:
                    # Replace the old entry in the data structure
                    self.db.skald.update_one(
                        {'_id': source['_id']},
                        {"$set": {
                            "ratings": R.tolist(),
                            "probabilities": S.tolist(),
                            "reputation": pe}}, upsert=False
                    )
                    # Output is set to None
                    output = None
                else:
                    # Create updated source
                    output = {
                        "sourceId": source['sourceId'],
                        "reputation": pe,
                        "probabilities": S.tolist(),
                        "ratings": R.tolist()
                    }
                # Return the output
                return output
        except Exception as e:
            # Log error
            logging.exception('An error occured while updating\
                               the reputation of the source: %s', e)

    def get_reputation(self, sourceId) -> float:
        """
        This function receives the id of a soure and searches
        for its reputation in the sources list. If the source
        exists, it returns the reputation score. Otherwise,
        the initial score is calculated and the source is added
        to the structure.

            Parameters:
                sourceId (int): Source identifier

            Returns:
                reputation: reputation score of the source
        """
        try:
            # Finds the source and creates it not exists
            source = self.db.skald.find_one_and_update(
                {'sourceId': sourceId},
                {'$setOnInsert': {
                    'sourceId': sourceId,
                    'reputation': self.point_estimate([1/self.k] * self.k),
                    'probabilities': [1/self.k] * self.k,
                    'ratings': [0] * self.k
                }},
                upsert=True,
                return_document=True
            )
            # Get the reputation
            reputation = source['reputation']
            # Return the reputation
            return reputation
        except Exception as e:
            # Log error
            logging.error('Error retrieving source reputation: %s', e)

    def get_source(self, sourceId):
        """
        This function receives the respective source from
        the storage.

            Parameters:
                sourceId (int): Source identifier

            Returns:
                source: entry of the source
        """
        try:
            source = self.db.skald.find_one({'sourceId': sourceId})
            # Check if the source exists
            if source is not None:
                # Log result
                logging.info("Source %s found in reputation module.",
                             sourceId)
                # Return source
                return source
            else:
                # Log result
                logging.info("Source %s not found in reputation module.",
                             sourceId)
                # Raise Exception
                raise Exception("Source %s was not found", sourceId)
        except Exception as e:
            # Log error
            logging.error('Error retrieving source reputation: %s', e)

    def clear_reputation(self):
        """
        This function has the sole purpose of deleting all
        the information regarding reputation for all sources.

        Returns:
                success: 1 for success, and -1 otherwise
        """
        try:
            # Clear the reputation
            self.db.skald.drop()
            # Return success
            return 1
        except Exception as e:
            # Log error
            logging.error('Error deleting reputation data: %s', e)
            # Return error
            return -1
