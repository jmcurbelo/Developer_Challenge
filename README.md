# Developer_Challenge

## About Project
This project is built with Python version 3.7 and Spark version 2.4.5. It was developed in PyCharm, but it can be run
on any computer that has Python installed in version 3.X or higher, and that also has Spark version 2.4.X or higher(See 
the Run the project section below).

## Generals
The project answers the following questions:
* Write one folder for each Nationality, each folder should contains the players with that Nationality.
* Know who are the 10 top players for each position, you should write this data too in another output folder.
* Know how many players for each position have each Club.
* Know the top 10 clubs for sprint speed average.
* Calculate the IMC for each player, and know all players with overweight (IMC>25).

The answers to these questions were made based on the data provided.

## About data
The data used was obtained from the url <https://www.kaggle.com/ekrembayar/fifa-21-complete-player-dataset> and refers to Football analytics, Sports Analytics, FIFA Series.

## Inputs
The inputs for this project are the aforementioned data.

## Outputs
The outputs of this project are 5 data frames. Each of these dataframes answers one of the challenge questions.
* **by_nationality:** Save all players partitioned by nationality.
* **ply_by_club_pos** Save how many players for each position each club has.
* **ply_overweight** Save those players who are overweight (IMC> 25).
* **top_spr_spd_avg** Save the top 10 clubs according to average sprint speed.
* **Falta**

## Run the project
* **In PyCharm:** Import the project and run the main class.
* **From CMD(on Windows) or Terminal(Linux or MacOs):**
    * Uncomment the following lines in the file `main.py`:
        * `import findspark`
        * `findspark.init()`
    * Save the file `main.py`
    * In your CMD/Terminal, go to the directory in which you downloaded the project and execute the following statement:
        * `python main.py`
