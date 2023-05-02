import os
import requests
import pandas as pd
import time

def create_instances(configs_path):
    conf_ls = os.listdir(configs_path)

    for conf in conf_ls:
        args = {'config': open(configs_path+conf, 'rb')}

        payload = {'name':conf.split('.')[0],
                    'accounts':'ipb-tadbir::7',
                    'algorithm':'Option-Runner-Algorithm'}

        headers = {
            'User-Agent': "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/109.0",
            'Accept': "application/json",
            'Accept-Language': "en-US,en;q=0.5",
            'Accept-Encoding': "gzip, deflate, br",
            # 'Content-Type': "multipart/form-data; boundary=---011000010111000001101001",
            'Origin': "https://isatis.hermesalgo.ir",
            'Connection': "keep-alive",
            'Referer': "https://isatis.hermesalgo.ir/dashboard",
            'Cookie': "token=Token+eyJhbGciOiJkaXIiLCJlbmMiOiJBMTI4Q0JDLUhTMjU2In0..dU2Iidkcet8XR8oUSbyi8A.6vNbA0LyF84KTpE9pqJ39IhiSf_uOsvBsHvjHfjmHCjUq2hzxwKo-E9rCyt8asNECp5fjKugSgfxwnxbKbwNHP4FE1hj3avRZ-iooCKD2z_E9nc5fYxKIg9_b52NrlCxr7SkH3wlfFgdh9zGsb1ZJavWyNUEf_xQXRdV-GrdAEsCN9hIgL6SYsQ2tcF3fzTXy8-_vEzgaBciGzzr0wGFUZij-rCr859J6bVzHKeT2KkOW6wgoNzyIMQGq-WfPlIG.Uq9jBF8iRUlOPtrbwr1urw",
            'Sec-Fetch-Dest': "empty",
            'Sec-Fetch-Mode': "cors",
            'Sec-Fetch-Site': "same-origin"
            }

        r = requests.post("https://isatis.hermesalgo.ir/web/api/v1/instances", headers=headers, files=args, data=payload)
        print(conf)
        print(r.content)
        print('-------------------------------')
        print()
        # time.sleep(2)