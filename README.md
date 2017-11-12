# twitchParser

This Parser fetches the data from the Twitch Chat API, given the Start and end timestamp of a VOD, it then formats it, removes useless words and keeps only relevant data for the python Interpreter to take care of.
This parsers uses up to 100 go routines that split evenly the work load.
