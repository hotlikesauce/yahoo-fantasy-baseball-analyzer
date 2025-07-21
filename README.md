# yahoo-fantasy-baseball-analyzer
![yahoofb](https://github.com/hotlikesauce/yahoo-fantasy-baseball-analyzer/assets/46724986/5a63122f-c5c9-4e21-ae7c-dfded8a2c26e)

## Description

This python web scraping project will help you aggregate your Yahoo Fantasy Baseball League stats and create datasets for power rankings, ELO calculations, season trends, and live standings. Additionally, it will create an expected wins dataset to give you an idea of an All-Play record on a week-by-week basis. This project has been created to write to a MongoDB but can be edited to use any database technology you would like.

Technologies Used: Python, MongoDB

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Installation

- pip install -r requirements.txt to install necessary dependencies

- Create a .env file with the following variables to help with obfuscation of passwords and emails
  - GMAIL = 'Your Email'<br>
  - GMAIL_PASSWORD = 'Your App Password From Gmail'<br>
  - MONGO_CLIENT = 'Your mongodb client string'
  - YAHOO_LEAGUE_ID = 'Your Yahoo Leage ID string (https://baseball.fantasysports.yahoo.com/b1/#####/)'
  - MONGO_DB = 'Your MongoDB Name'

- The way it's currently set up, you will need to manipulate your gmail account to allow for third party apps to send emails on your behalf for failure notifications

- You will need a manager_dict.py file containing the names of fantasy team managers in your league. This is used to create a team dictionary to handle fantasy team name changes throughout the course of the year. I have created a manager_dict_example.py file as a template.

## Usage

### Local Usage
- Run the Live Standings script every hour as a scheduled task to push data to MongoDB
- Run the Weekly Updates script every week after weekly scores are calculated

### AWS Lambda Deployment (Recommended)

Deploy your scripts to run automatically in the cloud:

```bash
# Quick setup and deployment
python setup.py      # Check prerequisites and install dependencies
python test_lambda.py # Test your functions locally
python deploy.py     # Deploy to AWS Lambda
```

This creates two Lambda functions:
- **Weekly Updates**: Runs every Sunday at 5am ET
- **Live Standings**: Runs every 15 minutes continuously

See [DEPLOYMENT.md](DEPLOYMENT.md) for detailed deployment instructions.

### Data Visualization
- Use MongoDB Charts with your data for live-updating dashboards
  - [Live Standings](https://charts.mongodb.com/charts-pc-kmmrs/dashboards/6435c9ca-38db-40b7-8761-892ed32c586e)
  - [Stat Analysis](https://charts.mongodb.com/charts-pc-kmmrs/dashboards/6435c9b3-8957-412e-8267-bed12f8caacb)

  - ![live](https://github.com/hotlikesauce/YahooFantasyBaseball_2023/assets/46724986/152959ea-8c2e-4ae6-82b3-079a53222f2b)
 
## Contributing

Please feel free to improve on my code and provide any optimizations you can create. I am always looking for improvements and recommendations on how to structure my code.

## License

[MIT License](https://choosealicense.com/licenses/mit/)

## Acknowledgments

Thank you to my league Summertime Sadness Fantasy Baseball for the stat ideas, the thousands of Stack Overflow posts I read, and ChatGPT.

## Contact

Please hit me up with questions or feedback. [My Email](mailto:taylorreeseward@gmail.com)
