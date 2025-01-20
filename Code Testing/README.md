# Open Capstone Code Testing

This segment of the project finalizes and runs tests on the code from Prototype Scaling Pyspark. The original code was developed on Databricks Notebooks and moved to the collector.py and transformer.py files for testing with pytest.
The test suite is contained within the file test_suite.py

## Running the tests
Running the tests requires ```pip install pytest``` and ```pip install coverage```

Running pytest:
```
pytest test_suite.py –v
```
Getting the coverage:
```
coverage run -m pytest test_suite.py -v
coverage report --omit="C:/spark/python/*,dbutils.py
```

## Results
After pytest:
![image](https://github.com/user-attachments/assets/4377e021-bcfe-45d7-ac3c-0727b2d4141e)

After coverage:
![image](https://github.com/user-attachments/assets/fb7b04b1-dce0-45ae-86b5-34851f2272e6)

