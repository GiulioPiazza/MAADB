import time
import json
import relational_db_utils as utils
 

# lettura del file di impostazioni
with open("settings.json") as json_file:
    setting_data = json.load(json_file)

def main():
        code = ""
        while code != "-1":
            print("\nSelect operation to do:")
            print("\tPress 1 to initialize database")
            print("\tPress 2 to run Tweets analysis")
            print("\tPress 3 to create word cloud")
            print("\tPress 4 to show resources stats")
            print("\tPress 5 to show words count plot")

            print("\t-1 to exit")

            code = input()

            # inizio a tracciare il tempo di esecuzione
            start_time = time.time()
            maria_db_setting = setting_data['MariaDB']

            if code == "1":
                utils.initialise_database(maria_db_setting)
            elif code == "2":
                utils.run_twitter_analysis(maria_db_setting)
            elif code == "3":
                utils.create_clouds(maria_db_setting)
            elif code == "4":
                utils.get_resources_stats(maria_db_setting)
            elif code == "5":
                utils.plot_counts(maria_db_setting)

            # mostro il tempo trascorso per l'esecuzione del task
            end_time = time.time()
            show_time(end_time - start_time)



def show_time(sec):
    mins = sec // 60
    sec = sec % 60
    hours = mins // 60
    mins = mins % 60
    print("Time Lapsed = {0}:{1}:{2}".format(int(hours), int(mins), int(sec)))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("An error has occourred:")
        print(e)
        print()
        input("Press a key to exit...")
        raise e