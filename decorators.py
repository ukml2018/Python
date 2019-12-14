from datetime import datetime
import os,shutil
from functools import wraps
import time,math

class Retry:
    @staticmethod
    # Retry decorator with exponential backoff
    def retry(tries, delay=3, backoff=2):
        """
        Retries a function or method until it returns True.
        delay sets the initial delay in seconds, and backoff sets the factor by which
        the delay should lengthen after each failure.
        """
        tries = math.floor(tries)
        if tries < 0:
            raise ValueError("tries must be 0 or greater")


        def deco_retry(f):
            @wraps(f)
            def f_retry(*args, **kwargs):
                mtries, mdelay = tries, delay  # make mutable
                err = None
                while mtries > 0:
                    print("Trial Number:" + str(mtries))
                    try:
                        rv = f(*args, **kwargs)
                    except DBException as e:
                        print("Retry..")
                        mtries -= 1  # consume an attempt
                        time.sleep(mdelay)  # wait...
                        mdelay += backoff  # make future wait longer
                        err = e

                    # except Exception as e:
                    #     print(str(e))
                    #     mtries -= 1  # consume an attempt
                    #     time.sleep(mdelay)  # wait...
                    #     mdelay += backoff  # make future wait longer
                    #     err = e
                    else:
                        return rv
                    raise err

            return f_retry  # true decorator -> decorated function

        return deco_retry  # @retry(arg[, ...]) -> true decorator

class DBException(Exception):
    def __init__(self,*args,**kwargs):
        Exception.__init__(self,*args,**kwargs)
