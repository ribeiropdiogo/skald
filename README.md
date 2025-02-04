# Skald: Improving Truth Discovery By Tracking Reputation

Traditional truth discovery approach estimate the truth from a set of values provided by multiple sources, which can be conflicting and heteregoneous. This repository contains **Skald**, a proof-of-concept of a solution that improves truth discovery with the addition of a reputation system. The architecture of Skald can be found in the Figure below, while a detailed explanation of the solution is present in the accompanying paper (To Be Published).

<p align="center">
    <img src="/doc/skald.png" width="600px">
<p>

## Getting Started

To maximize compatibility across all architectures, the deployment of Skald is done using **Docker**. The installation method for Docker is not relevant. Docker Desktop can be used (can be the most straightforward method), but installation through package managers is not an issue, as long as `docker compose` is available. More information on installing Docker can be found [here](https://docs.docker.com/get-started/get-docker/).

- Build the Skald image and launch the containers

    ```bash
    docker compose up --build -d
    ```

## Usage

After launching the containers, interaction with Skald is done through the API available at http://localhost:8000/docs. There are two endpoints at the moment: a `consolidate` endpoint, which performs the truth discovery process, and a `clear` endpoint, which clears the database of the reputation system. To send a request to the API, you can use tools such as [Postman](https://www.postman.com).

- A `POST` request to the `consolidate/` has the following format:
    ```json
    {
        "objects": [
            {
                "name": "name",
                "datatype": "string",
                "claims": [
                    {
                        "sourceId": "source1",
                        "fact": "Thomas A. Anderson"
                    },
                    {
                        "sourceId": "source2",
                        "fact": "Thomas Anderson"
                    },
                    {
                        "sourceId": "source3",
                        "fact": "Tommy A. Anderson"
                    },
                    {
                        "sourceId": "source4",
                        "fact": "Anderson"
                    }
                ]
            }
        ],
        "sources": []
    }
    ```
    - The `name` field is used to identify the value
    - The `datatype` field is used to specify the tyoe of the value (string, continuous, or categorical)
    - The `claims` list contains a list of values provided by the various sources
        - This list is composed by the pair `sourceId` and `fact`, corresponding to the identifier for the source providing the value, and the value itself, respectively.