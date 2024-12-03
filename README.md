This project provides a Docker image for running calculations based on input data stored in project directories.

## Build the Docker Image

Run the following command in the project directory to build the Docker image:

```bash
docker build -t rostislav_g_solution .
```

## Running the Calculations
Prepare Input Data:
Place your input data in the following directories within the project:

+ `./data/claims`
+ `./data/pharmacies`
+ `./data/reverts`
 
Run the Container:
Use the following command to run the container, specifying input paths and output directory:

```bash
docker run \
    -v ./data:/app/data \
    -v ./output:/app/output \
    --rm rostislav_g_solution \
        --input_claims /app/data/claims \
        --input_reverts /app/data/reverts \
        --input_pharmacies /app/data/pharmacies \
        --cores 10
```

## Output Files:
Processed output files will be written to the `./output` directory inside your project.

## Notes
+ Adjust the paths in the docker run command as needed to match your directory structure.
+ Ensure that the input directories (claims, pharmacies, reverts, etc.) contain the necessary data files.
+ The `--cores` argument specifies the number of CPU cores to be used for the calculations.