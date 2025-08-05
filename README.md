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

## Usage

### Local Usage
- Run the Live Standings script every hour as a scheduled task to push data to MongoDB
```bash
python get_live_standings.py
```
- Run the Weekly Updates script every week after weekly scores are calculated
```bash
python weekly_updates.py
```
- Functions which will be run as a part of the weekly updates
```bash
def main():
    functions = [
        get_live_standings_main
        ,get_season_trend_power_ranks_main 
        ,get_power_rankings_main 
        ,get_all_play_main 
        ,get_weekly_results
        ,get_season_trend_standings_main 
        ,get_weekly_prediction_main 
        ,get_elo
        ,get_season_results
        ,get_remaining_sos
    ]
```


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
  - [Live Standings](https://charts.mongodb.com/charts-pc-kmmrs/public/dashboards/9356d15c-ebdd-4ac3-a061-858c0a3b5026)
  
  - ![live](https://github.com/hotlikesauce/YahooFantasyBaseball_2023/assets/46724986/152959ea-8c2e-4ae6-82b3-079a53222f2b)
 
  - [Stat Analysis](https://charts.mongodb.com/charts-pc-kmmrs/public/dashboards/47923ebc-cab2-4670-96a0-cbb87193490a)

  - ![demo](https://github.com/hotlikesauce/YahooFantasyBaseball_2023/assets/46724986/5d4fcfeb-33ee-4dad-88d6-18de16486e26)

## Contributing

Please feel free to improve on my code and provide any optimizations you can create. I am always looking for improvements and recommendations on datasets which would be userful to compile.

## License

[MIT License](https://choosealicense.com/licenses/mit/)

## Acknowledgments

Thank you to my league Summertime Sadness Fantasy Baseball for the stat ideas, the thousands of Stack Overflow posts I read, and AI tools.

## Contact

Please hit me up with questions or feedback. [My Email](mailto:taylorreeseward@gmail.com)
