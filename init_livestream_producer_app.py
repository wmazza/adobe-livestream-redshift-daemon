#!/usr/bin/env python

#
# Multiple daemons for the same app
#

import sys, os, datetime, time
from livestream_producer_app import livestream_producer_app

dir_path = os.path.dirname(os.path.realpath(__file__)) + '/'

#check is anough arguments are passed
if len(sys.argv) != 3:
    print("usage: %s start|stop|restart <param>" % sys.argv[0])
    sys.exit(2)

#get the extra arguments
id = sys.argv[2]
print('Param (ID): ' + sys.argv[2])
#start the app with the parameters
daemon = livestream_producer_app(id)


#from the article by Sander Marechal
# http://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/
if len(sys.argv) == 3:
    if 'start' == sys.argv[1]:
        print('Start')       
        try:
            daemon.start()
        except ConnectionError as ce:
            with open(dir_path + "/data/logs/stderr.log", "a+") as stderr_log:
                stderr_log.write('Connection exception occurred: ' + str(e) + '.\nLivestream_producer_app to be restarted.\n')

        except Exception as e:
            with open(dir_path + "/data/logs/stderr.log", "a+") as stderr_log:
                stderr_log.write('Exception occurred: ' + str(e) + '.\nLivestream_producer_app to be restarted.\n')

    elif 'stop' == sys.argv[1]:
        daemon.stop()
        print('Stop')
    elif 'restart' == sys.argv[1]:
        try:
            print('Restarting...')
            daemon.restart()
            print('Restarted')
        except ConnectionError as ce:
            with open(dir_path + "/data/logs/stderr.log", "a+") as stderr_log:
                stderr_log.write('Connection exception occurred: ' + str(e) + '.\nLivestream_producer_app to be restarted.\n')
            time.sleep(1)

        except Exception as e:
            with open(dir_path + "/data/logs/stderr.log", "a+") as stderr_log:
                stderr_log.write('Exception occurred: ' + str(e) + '.\nLivestream_producer_app to be restarted.\n')
            time.sleep(1)
    else:
        print("Unknown command")
        sys.exit(2)
    sys.exit(0)
else:
    print("usage: %s start|stop|restart" % sys.argv[0])
sys.exit(2)