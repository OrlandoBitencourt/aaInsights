# AA Insights

This project is an AA Insights, which provides various functionalities for analyzing user logs within the ArcheRage game environment. The analyzer is built using Python and utilizes Streamlit for the web interface.

![logo300x300)](https://github.com/OrlandoBitencourt/aaInsights/assets/11688998/217771eb-1bae-41b7-a744-88a5d31a21af)

![logo](app/logo300x300.png)

## Project Structure

The project consists of several Python scripts:

- `front.py`: Contains the main functionality of the AA Insights.
- `cron/cron.py`: Contains a cron runner to import logs into database.

## Requirements

Before running the project, ensure you have the following dependencies installed:

- Python (>=3.6)
- Streamlit
- Pandas
- psycopg2-binary
- streamlit_option_menu

You can install the required dependencies using pip:

```bash
pip install -r requirements.txt
```

## Usage

To run the AA Insights, execute the following command in your terminal:

```bash
streamlit run front.py
```

To run the cron runner:
```bash
python cron.py
```

Also you can run in a container, just execute the ```compose.yaml``` file.

This will launch the Streamlit application, providing access to various functionalities for analyzing user logs.

## Functionality

The AA Insights provides the following functionalities:

- **Overview**: Provides an overview of the database, including total users and logs.
- **Users**: Allows users to view user data, faction distribution, user logs by location, and attendance.
- **Logs**: Offers various log analysis options, including an overview of logs, PvP damage, heals, and PvE damage.


## License

© 2024 Developed by Xizde. All rights reserved.

