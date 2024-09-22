# WealthWizard - Budget Management System

## Overview

WealthWizard is a budget management system designed to assist users in efficiently creating, editing, and managing their budgets. It offers features such as budget creation, editing, deletion, and viewing of budgets. Additionally, it provides visualization tools using Spark and Dash for budget analysis and insights.

## Key Features

1. **Budget Creation:**
   - Users can create new budgets by providing details such as:
     - User ID
     - Budget category
     - Budget amount
     - Start date and end date
   - Budgets can also be created in bulk by uploading a CSV file with the necessary information.

2. **Budget Editing:**
   - Users can modify existing budgets by updating any of the following fields:
     - Budget category
     - Budget amount
     - Start date and end date
   - Budgets are selected by their budget ID, and only those associated with the user's ID are editable.

3. **Currency Conversion:**
   - The system allows users to convert their budget amounts into different supported currencies, such as USD, INR, AUD, GBP, and CHF.

4. **Budget Deletion:**
   - Budgets can be deleted by specifying the budget ID or the user ID.
   - The system also supports deleting all budgets associated with a specific category for a user.

5. **Visualization and Insights:**
   - Using Dash and Plotly, the system provides users with interactive dashboards to visualize their budgets and spending patterns.
   - Visualizations include:
     - Spending breakdown by category
     - Budget vs. spent comparison
     - Spending trends over time

6. **CSV Upload:**
   - Multiple budget entries can be added by uploading a CSV file, which is processed and stored in the database.

## Program Flow

1. The program initializes with a connection to the Oracle database using credentials from the environment variables.
2. Users interact with a menu-based interface that offers different options such as:
   - Creating a new budget (single or multiple entries via CSV)
   - Updating an existing budget
   - Deleting budgets
   - Viewing all budgets associated with a user ID
   - Visualizing budget data
3. The user selects an option, and the system processes the corresponding functionality using the `BudgetManager` class.
4. If the visualization option is selected, the system launches a Dash web application that provides insights into the user's spending habits.
5. Finally, upon exiting, the program closes the database connection.

## Installation and Setup

### Prerequisites
- Oracle Database (with appropriate procedures created in SQL)
- Python 3.x
- Required Python packages listed in `requirements.txt`
- SQL Developer for managing the database

### Cloning the repo 
Run this in your terminal to clone the repo 
```bash
git clone https://github.com/anjanayraina/WealthWizardSquad3/
```
### Setting up the Environment Variables
Create an `.env` file in the root directory of the project and add the following variables. Replace the values with your database credentials:

```bash
USER_SYSTEM = "your_oracle_database_username"
PASSWORD = "your_password"
HOST = "your_host"
PORT = "your_port"
SID = "your_sid"
```

### Installing Required Packages

To install the necessary Python packages, ensure you are in the root directory of the project and run the following command:

```bash
pip install -r requirements.txt
```

### Running the Application
Once the environment is set up and the Oracle database is configured, you can run the main application by executing:

```bash
python main.py
```

This will start the budget management system, where you can create, edit, delete, and view budgets through an interactive menu.

### Running Tests and Code Coverage
We use Python's unittest module along with coverage to test the code and measure test coverage.

Setup .coverage.rc File
Create a .coverage.rc file in the root directory with the following content to exclude external libraries and test files from the coverage report:

```bash
[run]
omit =
    */usr/lib/python3/*
    */site-packages/*
    tests/*
```
### Running Tests
To run all tests in the tests directory, use the following command:

```bash
python3 -m unittest discover -s tests -p "*.py"
```
To run a specific test file 
```bash
python3 -m unittest discover -s tests -p "TestFileName.py"
```
### Measuring Test Coverage
To measure the test coverage, use this command:

```bash
python3 -m coverage run -m unittest discover -s tests -p "*.py"
```
To get the coverage of a specific file  
```bash
python3 -m coverage run -m unittest discover -s tests -p "TestFileName.py"
```
After running the coverage command, you can generate a coverage report by running:

```bash
python3 -m coverage report
```
This will display the test coverage for your project.

