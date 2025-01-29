"""
This module contains the code responsible for the implementation
of the TruthFinder algorithm proposed by Yin et al. used for data
consolidation purposes.
"""
import math
import warnings
import numpy as np
from numpy.linalg import norm
import textdistance as td

warnings.filterwarnings("error")


def euclidean_distance(x, y):
    return np.sqrt(sum(pow(float(a) - float(b), 2) for a, b in zip(x, y)))


def continuous_implication(f1, f2):
    # Get the maximum between f1 and f2
    max_value = max(f1, f2)
    # Calculate the euclidean distance
    distance = euclidean_distance([f1], [f2])
    # Normalize between -1 and 1
    i = 2 * ((float(distance) - float(max_value)) / (0 - float(max_value))) - 1
    # Return the normalized value
    return i


def categorical_implication(f1, f2):
    # Both facts are equal
    if f1 == f2:
        # If f1 is correct, f2 is likely to be correct
        return 1
    else:
        # The facts are differents
        # If f1 is correct, f2 is likely to be wrong
        return -1


def string_implication(f1, f2):
    # Calculate the similarity
    similarity = td.jaro_winkler(f1, f2)
    # Normalize between -1 and 1
    i = 2 * (similarity - 0.5)
    # Return the normalized value
    return i


def sigmoid(x):
    """
    This function is included to avoid possible negative confidence scores.

            Parameters:
                    x (float): Product of dampenign factor andfact confidence

            Returns:
                    confidence (float): Confidence score of the fact
    """
    # Avoid negative values (Eq. 8)
    return 1 / (1 + math.exp(-x))


class TruthFinder(object):
    """
    The TruthFinder represents the algorithm and includes all the necessary
    logic to comput te fact confidences and trustworthiness scores.

    Attributes
    ----------
    implication : function
        implication function returning a value between -1 and 1
    dampening_factor : float
        dampening factor
    influence_related : float
        influence between related facts

    Methods
    -------
    confidence_score(df):
        Calculates the confidence scores.
    adjusted_confidence_score(df):
        Calculates the adjusted confidence scores.
    compute_fact_confidence(df):
        Calculates the fact confidence.
    update_fact_confidence(df):
        Updates the fact confidence.
    update_source_trustworthiness(df):
        Updates the source trustworthiness.
    iteration(df):
        Performs an iteration.
    stop_condition(t1,t2,threshold):
        Verifies if stop condition is met.
    run(dataframe,max_iterations=200,threshold=1e-6,initial_trustworthiness=0.9):
        Executes the algorithm.
    """
    def __init__(self, dampening_factor=0.3, influence_related=0.5):
        # Verify if values are in the accepted interval
        assert 0 < dampening_factor < 1
        assert 0 <= influence_related <= 1
        # Initialize values
        self.dampening_factor = dampening_factor
        self.influence_related = influence_related

    def confidence_score(self, df):
        """
        This function calculates the confidence scores.

                Parameters:
                        df (dataframe): Dataframe containing the sources,
                            facts, objects, trustworthiness and fact confidence

                Returns:
                        df (datafram): Dataframe with new confidence scores
        """
        # Trustworthiness score of source (Eq. 3)
        trustworthiness_score = lambda x: - math.log(1 - x)
        # Calculate confidence for each fact
        for i, row in df.iterrows():
            # Get the trustworthiness score of each source providing f
            ts = df.loc[df["fact"] == row["fact"], "trustworthiness"]
            # Sum the trustworthiness score of each source providing f (Eq. 5)
            confidence_score = sum(trustworthiness_score(t) for t in ts)
            # Set the calculate confidence for the fact
            df.at[i, "fact_confidence"] = confidence_score
        # Return the data
        return df

    def adjusted_confidence_score(self, df):
        """
        This function calculates the adjusted confidence scores.

                Parameters:
                        df (dataframe): Dataframe containing the sources,
                            facts, objects, trustworthiness and fact confidence

                Returns:
                        df (datafram): Dataframe with adjusted confidence score
        """
        adjusted = {}
        # Iterate through the facts
        for i, row1 in df.iterrows():
            # Get the first fact
            f1 = row1["fact"]
            # Initialize the sum
            sum = 0
            # Iterate through the facts
            for j, row2 in df.drop_duplicates("fact").iterrows():
                # Get the second fact
                f2 = row2["fact"]
                # Compare the facts
                if f1 == f2:
                    # Ignore if they are the same facts
                    continue
                if row1["datatype"] == "continuous":
                    # Add product between confidence score and implication
                    sum += row2["fact_confidence"] * \
                        continuous_implication(float(f2), float(f1))
                elif row1["datatype"] == "string":
                    sum += row2["fact_confidence"] * \
                        string_implication(f2, f1)
                elif row1["datatype"] == "categorical":
                    sum += row2["fact_confidence"] * \
                        categorical_implication(f2, f1)
            # Calculate the adjusted confidence score (Eq. 6)
            adjusted[i] = self.influence_related * \
                sum + row1["fact_confidence"]
        # Iterate through the facts
        for i, row1 in df.iterrows():
            # Update the values of confidence scores
            df.at[i, "fact_confidence"] = adjusted[i]
        # Return the data
        return df

    def compute_fact_confidence(self, df):
        """
        This function calculates the fact confidence scores.

                Parameters:
                        df (dataframe): Dataframe containing the sources,
                            facts, objects, trustworthiness and fact confidence

                Returns:
                        df (datafram): Dataframe with new confidence scores
        """
        # Calculate the confidence of f (Eq. 8)
        s = lambda x: sigmoid(self.dampening_factor * x)
        # Iterate through facts
        for i, row in df.iterrows():
            # Calculate and update confidence of the fact
            df.at[i, "fact_confidence"] = s(row["fact_confidence"])
        # Return the data
        return df

    def update_fact_confidence(self, df):
        """
        This function iterates through the dataframe, calling all the functions
        required to calculate the confidence scores for each fact related to an
        object.

                Parameters:
                        df (dataframe): Dataframe containing the sources,
                            facts, objects, trustworthiness and fact confidence

                Returns:
                        df (datafram): Dataframe with confidence scores
        """
        # Iterate through every object
        for object_ in df["object"].unique():
            # Get indices for the object
            indices = df["object"] == object_
            # Get entries in df related to the object
            d = df.loc[indices]
            # Calculate the confidence scores
            d = self.confidence_score(d)
            # Calculate the adjusted confidence scores
            d = self.adjusted_confidence_score(d)
            # Calculate the fact confidences
            df.loc[indices] = self.compute_fact_confidence(d)
        # Return the data
        return df

    def update_source_trustworthiness(self, df):
        """
        This function iterates through the dataframe, calling all the functions
        required to calculate the source trustworthiness scores.

                Parameters:
                        df (dataframe): Dataframe containing the sources,
                            facts, objects, trustworthiness and fact confidence

                Returns:
                        df (datafram): Dataframe with source trustworthiness
        """
        # Iterate through the sources
        for source in df["source"].unique():
            # Get the indices for the source
            indices = df["source"] == source
            # Get the confidence scores of facts provided by the source
            cs = df.loc[indices, "fact_confidence"]
            # Sum confidence scores and divide by number of facts (Eq. 1)
            df.loc[indices, "trustworthiness"] = sum(cs) / len(cs)
        # Return the data
        return df

    def iteration(self, df):
        """
        This function performs an iteration of the algorithm.

                Parameters:
                        df (dataframe): Dataframe containing the sources,
                            facts, objects, trustworthiness and fact confidence

                Returns:
                        df (dataframe): Dataframe with updated fact confidence
                            and source trustworthiness
        """
        # Update the fact confidence
        df = self.update_fact_confidence(df)
        # Update the source trustworthiness
        df = self.update_source_trustworthiness(df)
        # Return the data
        return df

    def calculate_change(self, t1, t2):
        """
        This function returns the change in trustworthiness between
        two vectors.

                Parameters:
                        t1 (array): Trustworthiness scores before new iteration
                        t2 (array): Trustworthiness scores after new iteration

                Returns:
                        float: Relative change between two vectors
        """
        # Calculate the cosine similarity
        cosine = np.dot(t2, t1) / (norm(t2) * norm(t1))
        # Change in trustworthiness is measured as 1 - cosine similarity
        change = 1 - cosine
        # Return True if under threshold and False if over threshold
        return change

    def run(self, dataframe, max_iterations=200, threshold=1e-6):
        """
        This function executes the TruthFinder algorithm.

                Parameters:
                        dataframe (dataframe): Dataframe with data
                        max_iterations (int): Max number of iterations
                        threshold (float): Threshold stopping condition

                Returns:
                        dataframe: Dataframe containing original data
        """
        # Initialize fact_confidence
        dataframe["fact_confidence"] = np.zeros(len(dataframe.index))
        # Loop until max_iterations
        #for i in range(max_iterations):
            # Remove duplicates for sources and get trustworthiness for each
            #t1 = dataframe.drop_duplicates("source")["trustworthiness"]
        # Perform an iteration
        dataframe = self.iteration(dataframe)
            # Remove duplicates for sources and get trustworthiness for each
            #t2 = dataframe.drop_duplicates("source")["trustworthiness"]
            #try:
                # Calculate cosine similarity
            #    change = self.calculate_change(t1, t2)
                # Check if difference is below threshold
            #    if change < threshold:
                    # Exit loop and return data
            #        return dataframe
            #except Exception as e:
            #    print(e)
            #    print(dataframe)
        # Return data
        return dataframe
