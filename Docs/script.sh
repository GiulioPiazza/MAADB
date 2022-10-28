#!/bin/bash

shopt -s nocasematch
read -p " Execute script? (y/n): " response
if [[ $response == y ]]; then
    printf " Loading....\\n"
    for ((x = 0; x<3; x++)); do
        printf " Open %s Terminal\\n" $x
        if [[ $x ==  0 ]]; then
            osascript -e 'tell application "Terminal" to do script "cd /Users/pier/Desktop/MEO/Codice/NoSQL && /opt/anaconda3/envs/meo/bin/python main_MongoDB.py 0"' >/dev/null
        fi
        if [[ $x ==  1 ]]; then
            osascript -e 'tell application "Terminal" to do script "cd /Users/pier/Desktop/MEO/Codice/NoSQL && /opt/anaconda3/envs/meo/bin/python main_MongoDB.py 1"' >/dev/null
        fi
        if  [[ $x ==  2 ]]; then
            osascript -e 'tell application "Terminal" to do script "cd /Users/pier/Desktop/MEO/Codice/NoSQL && /opt/anaconda3/envs/meo/bin/python main_MongoDB.py "' >/dev/null
        fi    
    done
fi
shopt -u nocasematch