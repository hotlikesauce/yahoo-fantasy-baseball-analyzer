# Project Name

Yahoo Fantasy Baseball League Data Aggregator

## Description

This python web scraping project  will help you aggregate your Yahoo Fantasy Baseball League stats and create datasets for power rankings, season trends, and live standings. Additionally, it will create an expected wins dataset to give you an idea of an All-Play record on a week-by-week basis. This project has been created to write to a mongodb, but can be edited to use any database technology you would like.

Technologies Used: Python, mongodb

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

- The way it's currently set up, you will need to manipulate your gmail account to allow for third party apps to send emails on your behalf for failure notifications


## Usage

- One of the many ways you can use this project is to run the Live Standings script every hour as a scheduled task. That will push data to mongodb where you can then use a free tier of mongo db, and utilize tthe mongodb charts interface to make a live-updating table
  - [Live Standings](https://charts.mongodb.com/charts-pc-kmmrs/dashboards/6435c9ca-38db-40b7-8761-892ed32c586e)

![Live Standings](https://imgur.com/dez40NG)


- Additionally you can run the Weekly Updates script every week after weekly scores have been calculated which generatoes weekly standings, power rankings, and an all-play type coefficient.
  - [Stat Analysis](https://charts.mongodb.com/charts-pc-kmmrs/dashboards/6435c9b3-8957-412e-8267-bed12f8caacb)
## Contributing

Please feel free to improve on my code and provide any optimizations you can create. I am always looking for improvements and recommendataions on how to structure my code.

## License

[MIT License](https://choosealicense.com/licenses/mit/)

## Acknowledgments

Thank you to my league Summertime Sadness Fantasy Baseball for the stat ideas, the thousands of Stack Overflow posts I read, and ChatGPT.

## Contact

Please hit me up with questions or feedback. [My Email](mailto:taylorreeseward@gmail.com)
