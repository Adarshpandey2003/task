# Task: Aggregate Material Data

Your task is to create a program that can ingest tabular data in the form of xlsx files and output a single aggregated xlsx file. 
The provided xlsx files are:
- `materials.xlsx`: Contains a list of materials with their properties.
- `plants.xlsx`: Contains a list of manufacturing plants and the materials they store.
- `storage.xlsx`: Contains storage details for materials across different plants.
- `suppliers.xlsx`: Contains a list of suppliers and the materials they supply.
- `supplier-names.xlsx`: Contains mapping of supplier IDs to supplier names.
- `manufacturer-names.xlsx`: Contains mapping of manufacturer IDs to manufacturer names.

Your program should aggregate the data from these files and produce a single xlsx file named `result.xlsx`.
For the aggregation, infer the relationships between the different data based on common column names and IDs.
The output files should be structured according to the provided template file `result-template.xlsx`.

Ensure that the output file contains all relevant information from the input files, properly linked and formatted as per the template.

## Guidelines
- Use Kotlin/Python as the programming language.
- Utilize open-source libraries for reading and writing xlsx files (e.g., Apache POI for Kotlin, openpyxl or pandas for Python).
- Include comments in your code to explain the logic and flow.
- Provide instructions on how to run the program and any dependencies required.
- In case of problems or questions in regards to the data or task description, please document your assumptions clearly in the code comments.
- Ensure that the program handles potential errors gracefully, such as missing files or malformed data.

## Submission
- Submit the complete source code along with any necessary instructions to run the program.
- Include the generated `result.xlsx` file as part of your submission.