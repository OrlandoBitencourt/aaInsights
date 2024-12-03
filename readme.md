# AA Insights

This project is an AA Insights, which provides various functionalities for analyzing user logs within the ArcheRage game environment. The analyzer is built using Python and utilizes Streamlit for the web interface.

App Preview: https://youtu.be/9tgOi_bWenc 
Timeline feature preview: https://youtu.be/aDkk0lr7hws

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
- schedule (cron)

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
- **Import**: Allows users to import manually log files, converting the files to database default timezone.

The cron import and update data in the database, theres a job that import logs and users, one job to convert data from halcy fights to set user factions based on halcy activity and another job to set mob faction based on the user_name, most of the mobs have ' ' a empty space character in name.

## License

© 2024 Developed by Xizde. All rights reserved.

## SHOUT OUT TO
<div class="row">
    <div class="col text-center">
        <img src="app/united_east_logo.png" alt="United East" width="80">
        <br>United East
    </div>
</div>

