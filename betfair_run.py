from log_notify import notify

from imp import reload
import match_odds_uploader
reload(match_odds_uploader)
from match_odds_uploader import downloader

notify = notify()

sport = 'tennis'#input('Enter Sport (lower case): ')
refresh = 10#int(input('Enter refresh: '))
hours = 8#int(input('Enter hours: '))
delay = 5#int(input('Enter delay: '))
matched_lower_bound = 10000#int(input('Enter min matched: '))





downloader = downloader(sport)


try:
    downloader.download(refresh,hours,delay,matched_lower_bound)
except Exception as e:
    notify.send_message(e,'odds_data')



#also wanna make this kick off the score downloader for the sport