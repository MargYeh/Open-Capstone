# Data Exploration

The dataset contains data pulled from the American Community Survey 5-Year Data (2009-2022)
- https://www.census.gov/data/developers/data-sets/acs-5year.html
- https://api.census.gov/data/2022/acs/acs5/profile/variables.html

Groups of Interest: https://censusreporter.org/topics/table-codes/
- Group DP02. Citizenship, Education, Household composition
- Group DP03. Selected Economic Characteristics: County level economic characteristics, income of various households, retirement, social security, health insurance
- Group DP04. Rent and Housing information
- Group S0804. Means of Transportation to Work by Selected Characteristics for Workplace Geography
- Group S0701. Geographic Mobility by Selected Characteristics in the United States
- Group S2303. Work status in the past 12 months
- Group CP03. Comparative Economic Characteristics

Year ranges from 2009 - 2022
##
## Sample Screenshots of Data:
![image](https://github.com/user-attachments/assets/b07ac9b4-3077-4306-a2bb-fa5a53687abd)
![image](https://github.com/user-attachments/assets/7171b40f-6b86-490c-a20b-9e2416811e60)

## Questions to Answer
The Data Exploration was done in Data_US_Census.ipynb and within table view in data.census.gov (Example: https://data.census.gov/table/ACSDP1Y2023.DP02?q=dp02)
1. Is the data homogenous in each column?
   > Yes, within the column. If there are empty values, then the whole column is empty. 

3. How do you anticipate this data will be used by data analysts and scientists downstream?
   > Data analysts can draw correlations between the data of different groups. For example, with the population group, people can separate out demographics in each county and see how they correlate with the target they are studying. They may also be interested in making conclusions based off the trends from multiple years.
3. Does your answer to the last question give you an indication of how you can store the data for optimal querying speed and storage file compression?
   > It hints that people might want to be able to access datasets from multiple groups at the same time. Maybe storing with a cloud service and using cloud analytics is the best method of optimizing both the speed and storage. To query across different years, the seperate jsons per year need to be combined together.
4. What cleaning steps do you need to perform to make your dataset ready for consumption?
   > Removal of empty columns and columns that provide no information. Replace the coded headers or put the description of the coded headers somewhere it is easy to look up. 
6. What wrangling steps do you need to perform to enrich your dataset with additional information?
   > The datasets can be joined so that not so many files need to processed. Maybe in each group, join all the years into one table.

## Thoughts
There are some missing columns, and the headers are coded. Descriptions of the codes are included in the dataset documentation but not in the dataset itself. I plan to keep most of the information untouched, but remove redundencies between many of the different groups. I also want to combine the data for each year into one single table for easy querying. I may also include a table that holds information connecting the header code to a description, so that the header codes can be easily looked up.

## Proposed Entity-Relationship Model
![image](https://github.com/user-attachments/assets/3ff0186f-2fa2-4a1b-9b06-76b992377807)


