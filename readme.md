# All-Cause Mortality Analysis of data from CPZP and OZP

A Python project for analyzing and visualizing CPZP and OSP dataset results

## How to run
The project was developed with Python 3.13, but it should work with any version >= 3.11.
### Setting up virtual enviroment (Optional but Recommended)
If you do not want to setup a virtual environment, you can run the project without it. Just proceed to the [Installing dependencies](#installing-dependencies) section.
1. Create a virtual environment using `venv`:

```bash
python -m venv .venv
```

2. Activate the virtual environment:
- Windows: `.\.venv\Scripts\activate`
- Linux/Mac: `source .venv/bin/activate`

### Installing dependencies
All needed requirements are listed in the `requirements.txt` file. You can install them using pip:

```bash
pip install -r requirements.txt
```


> **Note:** The requirements.txt file contains also dev dependencies, which are not required to run the project. (linters, formatters, tests, etc.) 
> The only required packages are polars, matplotlib, pyarrow and psutil


### Running application
The project consists of three main files

1. **Preprocessing script**
- Preprocesses data from the provided dataset (**CPZP.csv** and **OZP.csv**).
- Does not have to be run, as the preprocessed data is already included in the `data/` folder (for more info, view [Preprocessed Data Availability](#optimization-and-system-requirements) section).
- Example usage:

```bash
python data_preprocessor.py
```


2. **HVE Simulation**
- Runs the HVE simulation and processes the results.
- Does not have to be run, as the preprocessed data is already included in the `data/` folder (for more info, view [Preprocessed Data Availability](#optimization-and-system-requirements) section).
- Example usage:

```bash
python simulation.py
```


3. **Data Visualizer (Graph maker)**
- Loads the preprocessed data and visualizes the results.
- Example usage:

```bash
python visualizer.py
```

## How does it work
### Input files structure
#### CPZP.csv
This file contains vaccination and mortality data with the following format:

| tyden_narozeni | POHLAVI | vakcina1_tyden | vakcina1_kod | vakcina2_tyden | vakcina2_kod | ... | vakcina5_tyden | vakcina5_kod | tyden_umrti |
| -------------- | ------- | -------------- | ------------ | -------------- | ------------ | --- | -------------- | ------------ | ----------- |
| YYYYWww        | Z/M     | YYYYWww        | int          | YYYYWww        | int          | ... | YYYYWww        | int          | YYYYWww     |

- `tyden_narozeni` – Birth week (YYYYWww format).
- `POHLAVI` – Gender (Z = Female, M = Male).
- `vakcinaX_tyden` – Week of the Xth vaccine dose (YYYYWww format).
- `vakcinaX_kod` – Vaccine type code (integer).
- `tyden_umrti` – Death week (if applicable) (YYYYWww format).


#### OZP.csv
This file follows a slightly different format:

| id_poj | pohlavi | rok_narozeni | mesic_narozeni | rok_umrti | mesic_umrti | vakcina1_kod | vakcina1_rok | vakcina1_mesic | vakcina2_kod | vakcina2_rok | vakcina2_mesic | vakcina3_kod | vakcina3_rok | vakcina3_mesic | vakcina4_kod | vakcina4_rok | vakcina4_mesic | vakcina5_kod | vakcina5_rok | vakcina5_mesic | vakcina6_kod | vakcina6_rok | vakcina6_mesic | vakcina7_kod | vakcina7_rok | vakcina7_mesic |
| ------ | ------- | ------------ | -------------- | --------- | ----------- | ------------ | ------------ | -------------- | ------------ | ------------ | -------------- | ------------ | ------------ | -------------- | ------------ | ------------ | -------------- | ------------ | ------------ | -------------- | ------------ | ------------ | -------------- | ------------ | ------------ | -------------- |
| int    | 0/1     | YYYY         | MM             | YYYY      | MM          | int          | YYYY         | MM             | int          | YYYY         | MM             | int          | YYYY         | MM             | int          | YYYY         | MM             | int          | YYYY         | MM             | int          | YYYY         | MM             | int          | YYYY         | MM             |


- `id_poj` – Unique person ID.
- `pohlavi` – Gender (0 = Female, 1 = Male).
- `rok_narozeni`, `mesic_narozeni` – Birth year and month.
- `rok_umrti`, `mesic_umrti` – Death year and month (if applicable).
- `vakcinaX_kod` – Vaccine type code (integer).
- `vakcinaX_rok`, `vakcinaX_mesic` – Year and month of the Xth vaccine dose.

### Person period dataframe structure
Since the input files have different formats, they are unified into a **person period dataframe**, which follows this schema:


 | PERSON_ID | TIME_PERIOD | TIME_PERIOD_INDEX | BIRTHDATE | DOSE_1 | DOSE_2 | DOSE_3 | DOSE_4 | DEATH_INDEX |
 | --- | --- | --- | --- | --- | --- | --- | --- | --- |
 | UInt32 | Utf8 | Int64 | Utf8 | Int64 | Int64 | Int64 | Int64 | Int64 | 

**Column Descriptions:**
- `PERSON_ID` – Unique identifier for a person.
- `BIRTHDATE` – Birthdate in YYYY-MM-DD format.
- `DOSE_X` – Index of the time period in which the Xth vaccine dose was administered.
- `DEATH_INDEX` – Index of the time period in which the person died (if applicable).
- `TIME_PERIOD` – A human-readable time period label.
- `TIME_PERIOD_INDEX` – Numeric index of the time period, making it easier to work with sequences without referencing dates manually.

This transformation is handled by PersonPeriodTemplateGenerator, which takes an input file and applies the neccessary logic defined in the unit-tests.


### Processed Results DataFrame Structure
Once the data is standardized, we can apply the actual logic and generate the **processed results dataframe**, which serves as the final dataset for analysis. 

The processed results dataframe is created by performing a cross product (or cross join) between the list of time periods and the list of people. This means that for each person, data is generated for every time period in the dataset. The Age, Death, and Vaccine Status are derived by simple logic described in folder `common/polars_expressions`.

The structure of the processed results dataframe is as follows:

 | PERSON_ID | TIME_PERIOD | AGE | DEATH_STATUS | VACCINE_STATUS | 
 | --- | --- | --- | --- | --- | 
 | UInt32 | Utf8 | Utf8 | Utf8 | Int64 | 

**Column Descriptions:**
- `PERSON_ID` – Unique identifier for a person.
- `TIME_PERIOD` – The analyzed time period (formatted as a string).
- `AGE` – Age category at the given time period:
    - <60
    - 60-69
    - 70-79
    - 80+
- `DEATH_STATUS` – The person's status related to death:
    - alive – The person is alive.
    - died_now – The person died in this time period.
    - after_death – The person was already dead before this time period.
- `VACCINE_STATUS` – Encoded vaccination status:
    - 0 – Unvaccinated
    - 10 – Less than 4 weeks after dose 1
    - 11 – More than 4 weeks after dose 1
    - 20 – Less than 4 weeks after dose 2
    - 21 – More than 4 weeks after dose 2
    - 30 – Less than 4 weeks after dose 3
    - 31 – More than 4 weeks after dose 3
    - 40 – Dose 4 or higher

This dataset serves as the foundation for further analysis and visualization.

### ACM Calculation 
Now when we have prepared and preprocessed the data to our needs, we can calculate the All-Cause Mortality (ACM) for each time period.

The ACM (All-Cause Mortality) calculation is performed using the `ACMCalculator` class. The logic for computing ACM for a given dataset follows these steps:

- `alive_person_weeks`: This counts the number of alive person-weeks (rows where death_status is alive), filtered by age group and vaccination status.
- `total_deaths`: This counts the total number of deaths (rows where death_status is died_now), filtered by age group and vaccination status.
- `num_of_time_periods_in_year`: The number of periods in a year, either 12 (months) or 52 (weeks), depending on the dataset granularity.

$$\text{ACM} = \frac{\text{total-deaths}}{\text{alive-person-weeks}}$$

$$\text{ACM per 100,000 people-years} = \text{ACM} \cdot 100,000 \cdot \text{num-of-time-periods-in-year}$$




### Visualization
The visualization is fairly simple process, by having ACM calculated per each vaccine status and age group, we create a simple bar chart to show the ACM trend across categories.


## Optimization and System requirements
The format of the preprocessed dataframe is not optimized for minimal memory usage or maximum speed. Instead, it is designed to be easily **debuggable** and **testable**.

### Memory Usage
- Preprocessing the CSV data requires up to 18GB of RAM when processing both datasets at once:
    - **CPZP dataset**: ~12GB RAM
    - **OZP dataset**: ~6GB RAM
- However, the final processed results dataframe is really small (around 3MB when compressed on disk), making it fast to load and analyze.

### Preprocessed Data Availability
The preprocessed dataframes for **OZP** and **CPZP** are already included in the `data/` folder. This means:

✅ Running the data processor is not required for visualization or data analysis.
✅ You can directly use the preprocessed files for further computations.


If you have any suggestions or improvements, please open an issue or pull request.