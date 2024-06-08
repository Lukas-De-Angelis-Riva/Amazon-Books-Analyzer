SERVER_IP = 'ClientHandler'
SERVER_PORT = 12345
RESULT_IP = 'ResultHandler'
RESULT_PORT = 12345


def set_up_config(path, **kargs):
    with open(path, 'w') as f:
        s = "[DEFAULT]\n"
        for k, v in kargs.items():
            s += f"{k} = {v}\n"
        f.write(s)


# CLIENT
set_up_config('client/config.ini',
              SERVER_PORT=SERVER_PORT,
              SERVER_IP=SERVER_IP,
              RESULT_PORT=RESULT_PORT,
              RESULT_IP=RESULT_IP,
              LOGGING_LEVEL='INFO',
              CHUNK_SIZE_BOOK=1200,
              BOOK_FILE_PATH='data/books_data.csv',
              CHUNK_SIZE_REVIEW=200,
              REVIEW_FILE_PATH='data/books_rating.csv',
              RESULTS_PATH='results.csv')

# CLIENT HANDLER
set_up_config('server/clientHandler/config.ini',
              SERVER_PORT=SERVER_PORT)

# QUERY 1
# ## WORKER
set_up_config('server/query1/worker/config.ini',
              LOGGING_LEVEL='INFO',
              CHUNK_SIZE=1200,
              PUBLISHED_DATE_MIN=2000,
              PUBLISHED_DATE_MAX=2023,
              CATEGORY='computers',
              TITLE='distributed')
# ## SYNCH
set_up_config('server/query1/synchronizer/config.ini',
              LOGGING_LEVEL='INFO',
              CHUNK_SIZE=5000)

# QUERY 2
# ## WORKER
set_up_config('server/query2/worker/config.ini',
              LOGGING_LEVEL='INFO',
              MIN_DECADES=10,
              CHUNK_SIZE=2500)
# ## SYNCH
set_up_config('server/query2/synchronizer/config.ini',
              LOGGING_LEVEL='INFO',
              CHUNK_SIZE=5000)

# QUERY 3
# ## WORKER
set_up_config('server/query3/worker/config.ini',
              LOGGING_LEVEL='INFO',
              CHUNK_SIZE=850,
              MINIMUN_DATE=1990,
              MAXIMUN_DATE=1999)

# ## SYNCH
set_up_config('server/query3/synchronizer/config.ini',
              LOGGING_LEVEL='INFO',
              CHUNK_SIZE=900,
              MIN_AMOUNT_REVIEWS=500,
              N_TOP=10)

# QUERY 5
# ## WORKER
set_up_config('server/query5/worker/config.ini',
              LOGGING_LEVEL='INFO',
              CHUNK_SIZE=1500,
              CATEGORY='Fiction')

# ## SYNCH
set_up_config('server/query5/synchronizer/config.ini',
              LOGGING_LEVEL='INFO',
              CHUNK_SIZE=1800,
              PERCENTILE=90)

# RESULT HANDLER
set_up_config('server/resultHandler/config.ini',
              LOGGING_LEVEL='INFO',
              SERVER_PORT=RESULT_PORT,
              SERVER_IP=RESULT_IP,
              FILE_NAME='results.csv')
