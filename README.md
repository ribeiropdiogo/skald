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

## Example Usage
