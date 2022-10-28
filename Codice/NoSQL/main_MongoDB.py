import time
import json
import mongo_db_utils as m_du
import mongo_primary_node as m_pa, mongo_secondary_node as m_sa
import sys

# lettura del file di impostazioni
with open("settings.json") as json_file:
    setting_data = json.load(json_file)

def main(argv):
        if argv == -1:
            code = ""
            while code != "-1":
                print("\nSelect operation to do:")
                print("\tPress 1 to initialize database")
                print("\tPress 2 to run Tweets analysis")
                print("\tPress 3 to create word cloud")
                print("\t-1 to exit")

                code = input()

                # inizio a tracciare il tempo di esecuzione
                start_time = time.time()
                mongo_db_setting = setting_data['MongoDB']

                if code == "1":
                    m_du.initialise_cluster(mongo_db_setting)
                elif code == "2":
                    m_pa.run_twitter_analysis(mongo_db_setting)
                elif code == "3":
                    m_du.create_clouds(mongo_db_setting)

                # mostro il tempo trascorso per l'esecuzione del task
                end_time = time.time()
                show_time(end_time - start_time)

        else:   
            # se sono nodi secondari, avvio il servizio che rimane in ascolto
            secondary_setting_data = setting_data['MongoDB']['SecondaryNodes'][int (argv)]

            m_sa.start_secondary_node(secondary_setting_data['Port'],
                                    secondary_setting_data['Address'],
                                    secondary_setting_data['DBPort'])
               



def show_time(sec):
    mins = sec // 60
    sec = sec % 60
    hours = mins // 60
    mins = mins % 60
    print("Time Lapsed = {0}:{1}:{2}".format(int(hours), int(mins), int(sec)))


if __name__ == "__main__":
    try:
        if (len(sys.argv) > 1):
            main(sys.argv[1])
        else:
            main(-1)
    except Exception as e:
        print("An error has occourred:")
        print(e)
        print()
        input("Press a key to exit...")
        raise e