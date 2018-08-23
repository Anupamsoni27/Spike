import site
from html.parser import HTMLParser
import logging
import pytz
import datetime
import smtplib
import mimetypes
from email import encoders
import glob, csv, xlwt, os
import xml.etree.ElementTree as et
from datetime import date, timedelta, datetime
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.audio import MIMEAudio
from email.mime.multipart import MIMEMultipart
import pandas as pd
import pyexcel as p
from pandas.tseries.holiday import USFederalHolidayCalendar
import pandas as pd
from pandas.tseries.offsets import CustomBusinessDay

tz = pytz.timezone('Europe/London')
us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())
bd_list = pd.DatetimeIndex(start='1960-01-01', end='2050-12-31', freq=us_bd)
today_ = datetime.now().strftime("%Y%m%d")
import itertools

server1 = "//10.0.9.58/gdmx/loaderGdmx/"
server2 = "//10.0.9.64/gdmx/loaderGdmx/"
server3 = "//10.0.9.91/gdmx/loaderGdmx/"
test_server = "//10.0.9.61/gdmx/loaderGdmx/"
# test_server = "C:\\Users\\anupam.soni\\PycharmProjects\\Spike\\testing\\allfiles\\"
t_path ="C:/Users/anupam.soni/PycharmProjects/spike/"
class MVvsGDM:
    def __init__(self, name):
        self.name = name

    def make_result(self):
        try:
            print("Running make_result")
            result_df_1.to_csv("tempresult/spike.csv", index=False)
            result_df_2.to_csv("tempresult/WARNING.csv", index=False)
            # result_df_3.to_csv("tempresult/NOISSUE.csv", index=False)
            # result_df_4.to_csv("tempresult/MISSING.csv", index=False)
        except:
            pass

    def merger(self):
        print("Running merger")
        try:

            wb = xlwt.Workbook()
            for filename in glob.glob("tempresult/*.csv"):
                (f_path, f_name) = os.path.split(filename)
                (f_short_name, f_extension) = os.path.splitext(f_name)
                ws = wb.add_sheet(f_short_name)
                spamReader = csv.reader(open(filename, 'rt'))
                for rowx, row in enumerate(spamReader):
                    for colx, value in enumerate(row):
                        ws.write(rowx, colx, value)
            wb.save("compiled.xls")
            p.save_book_as(file_name='compiled.xls',
                           dest_file_name='reports/REPORT_test1.xlsx')
        except:
            pass

    def make_global(self):
        print("Running make global")
        global spike_issue_found
        spike_issue_found = 0
        print("spike_issue_found" + str(spike_issue_found))

        global warning
        warning = []
        global df
        global result_df_1
        global result_df_2
        global result_df_3
        global result_df_4
        result_df_1 = pd.DataFrame(
            columns=["MV MODEL DESCRIPTION", "DG MODEL CODE ", "DG MODEL DESCRIPTION", "PROFILE", "MV VALUE",
                     "DG VALUE", "OBSERVATION", "ACTION"])  # MISSMATCH
        result_df_2 = pd.DataFrame(
            columns=["MV MODEL DESCRIPTION", "DG MODEL CODE ", "DG MODEL DESCRIPTION", "PROFILE", "MV VALUE",
                     "DG VALUE", "OBSERVATION", "ACTION"])  # SPIKEISSUE
        result_df_3 = pd.DataFrame(
            columns=["MV MODEL DESCRIPTION", "DG MODEL CODE ", "DG MODEL DESCRIPTION", "PROFILE", "MV VALUE",
                     "DG VALUE", "OBSERVATION", "ACTION"])  # FORALLINFO
        result_df_4 = pd.DataFrame(
            columns=["MV MODEL DESCRIPTION", "DG MODEL CODE ", "DG MODEL DESCRIPTION", "PROFILE", "MV VALUE",
                     "DG VALUE", "OBSERVATION", "ACTION"])  # MISSING
        global df1_list
        global df2_list
        global df3_list
        global df4_list
        df1_list = []
        df2_list = []
        df3_list = []
        df4_list = []

        global validation_list
        validation_list = []
        global e_present
        e_present = 0
        global f_present
        f_present = 0
        global p_present
        p_present = 0
        global final_list
        final_list = []
        global file_list_1
        file_list_1 = []

    def gdm(self):
        print("Running gdm")

        global dg_file_present_count
        global dg_process_signal
        dg_file_present_count = 0
        dg_process_signal = 1

        # if date.today().weekday() == 0:
        #     delta = 3
        #     print(delta)
        # else:
        #  delta = 1
        last_weekday = date.today()
        temp_date = last_weekday.strftime('%Y%m%d')
        file_list = []
        servers = [test_server ]
        # servers = ["testing/files"]
        for server in servers:
            print("searching for date : " + temp_date + " DataSet : " + dg_full_product_code + " in Server : " + server)
            for root, dir, files in os.walk(server + dg_full_product_code):
                print(root,files)
                for file in files:
                    # print(file)
                    # print("searching for "+ temp_date)
                    if temp_date in file and file_to_read in file and  dg_full_product_code in file:

                        if "EOD" in file and file_to_read == "EOD":
                            print(file_to_read)
                            status = "_P_"
                            dg_file_present_count = 1
                        elif "ESET" in file and file_to_read == "ESET":
                            print(file_to_read)

                            status = "_E_"
                            dg_file_present_count = 1
                        elif "OI" in file and file_to_read == "OI":
                            print(file_to_read)

                            status = "_F_"
                            dg_file_present_count = 1
                        file_list.append(file)
                        data = []

                        try:
                            # with open("output_" + temp_date + "_" + status + "_" + category + ".csv", 'a', newline="") as f:
                            tree = et.parse(server + dg_full_product_code + "/" + file)
                            root = tree.getroot()

                            for models in root:
                                # print(models_uri)
                                profile_list = []
                            for childs in models:
                                if childs.tag == "profiles":
                                    for profile in childs:
                                        profile_list.append(profile.attrib['name'])
                            for temp_profile in profile_list:
                                for models in root:
                                    models_uri = models.attrib['uri']
                                    for childs in models:
                                        if childs.tag == "properties":
                                            for property in childs:
                                                if property.attrib["name"] == "MODELDESC":
                                                    model_description = property.attrib["value"]
                                        if childs.tag == "profiles":
                                            for profile in childs:
                                                if profile.attrib['name'] == temp_profile:
                                                    # print(profile.attrib)
                                                    for timeseries in profile:
                                                        if timeseries.tag == "timeseries":
                                                            for observation in timeseries:
                                                                # print(observation.attrib)
                                                                # print(models_uri, temp_profile,
                                                                #       observation.attrib['value'])
                                                                row = models_uri, temp_profile, observation.attrib[
                                                                    'value'], model_description, dg_full_product_code, temp_date, dg_product_code, mv_product_code
                                                                data.append(row)
                                                                df = pd.DataFrame(data)

                            df.to_csv(
                              t_path+  "output/output_" + temp_date + "_" + status + "_$" + dg_full_product_code + ".csv",
                                index=False)
                        except:
                            print(file_to_read+" file not found in " + server)
                            pass
        if dg_file_present_count == 1:
            print(file_to_read+" files found in server " + server + " for " + dg_full_product_code)
        elif dg_file_present_count == 0:
            print("File not present for DataSet : " + dg_full_product_code + " date : " + temp_date)
            warning.append(file_to_read+" file not present for DataSet : " + dg_full_product_code + " date : " + temp_date)
            dg_process_signal = 1
        print("dg_file_present_count " + str(dg_file_present_count))
        print("dg_process_signal " + str(dg_process_signal))

    def MV(self):
        print("")

        for root, dir, files in os.walk(t_path+"output"):
            # print(root)
            # print("DG FULL PRO: "+ dg_full_product_code )
            for file in files:
                if "_E_" in file and dg_full_product_code in file:
                    with open(root + "/" + file, 'r') as s:
                        reader = csv.reader(s)
                        model_list = []
                        mv_model_list = []
                        i = 0

                        for r in reader:
                            # print(r)
                            if "REGULAR" in r[0] and ".M" in r[0]:
                                model_list.append(r[0][-3:])

                                # print(r[0][-3:])
                        # print(len(model_list))
                        model_list = set(model_list)
                        model_list = list(model_list)
                        model_list.sort()
                        if "YES" in bracket or "yes" in bracket or "Yes" in bracket:
                            for model in model_list:
                                # print(model)
                                if len(str(i)) == 1:
                                    str_i = "0" + str(i)
                                else:
                                    str_i = str(i)

                                mv_model_list.append(mv_product_code + "[" + str_i + "]")
                                # print(mv_product_code + "[" + str_i + "]")
                                i = i + 1
                        elif "No" in bracket or "no" in bracket or "NO" in bracket:
                            for model in model_list:
                                if len(str(i)) == 1:
                                    str_i = "0" + str(i)
                                else:
                                    str_i = str(i)
                                # print(model)
                                mv_model_list.append(mv_product_code + str_i)
                                # print(mv_product_code + str_i )
                                i = i + 1

            # if dg_process_signal == 1:
            df = pd.DataFrame()
            settle_data = pd.DataFrame()
            all_data = pd.DataFrame()
            oi_data = pd.DataFrame()
            for root, dir, files in os.walk("output"):
                for file in files:
                    count = 0
                    if "__E__$" in file and dg_full_product_code + ".csv" == file.split("$")[1]:
                        # print(file)
                        df1 = pd.read_csv(root + "/" + file)
                        settle_data["model"] = df1[["0"]]
                        settle_data["profile"] = df1[["1"]]
                        settle_data["values"] = df1[["2"]]
                        settle_data["model description"] = df1[["3"]]
                        settle_data["DG_FULL_PRODUCT_CODE"] = df1[["4"]]
                        settle_data["DATE"] = df1[["5"]]
                        settle_data["DG_PRODUCT_CODE"] = df1[["6"]]
                        settle_data["MV_PRODUCT_CODE"] = df1[["7"]]

                        # print(settle_data)
                    elif "__F__$" in file and dg_full_product_code + ".csv" == file.split("$")[1]:
                        count = 1
                        df3 = pd.read_csv(root + "/" + file)
                        oi_data["model"] = df3[["0"]]
                        oi_data["profile"] = df3[["1"]]
                        oi_data["values"] = df3[["2"]]
                        oi_data["model description"] = df3[["3"]]
                        oi_data["DG_FULL_PRODUCT_CODE"] = df3[["4"]]
                        oi_data["DATE"] = df3[["5"]]
                        oi_data["DG_PRODUCT_CODE"] = df3[["6"]]
                        oi_data["MV_PRODUCT_CODE"] = df3[["7"]]

                        oi_data = oi_data[
                            (oi_data.profile == "OI")]
                    elif "__P__$" in file and dg_full_product_code + ".csv" == file.split("$")[1]:

                        df2 = pd.read_csv(root + "/" + file)
                        all_data["model"] = df2[["0"]]
                        all_data["profile"] = df2[["1"]]
                        all_data["values"] = df2[["2"]]
                        all_data["model description"] = df2[["3"]]
                        all_data["DG_FULL_PRODUCT_CODE"] = df2[["4"]]
                        all_data["DATE"] = df2[["5"]]
                        all_data["DG_PRODUCT_CODE"] = df2[["6"]]
                        all_data["MV_PRODUCT_CODE"] = df2[["7"]]

                        # all_data["DG_PRODUCT_CODE"] = df1[[""]]

                        if count == 1:
                            # print(all_data)
                            all_data = all_data[(all_data.profile == "HIGH") | (all_data.profile == "LOW") | (
                                        all_data.profile == "SETTLE") | (
                                                        all_data.profile == "OPEN") | (all_data.profile == "CLOSE") | (
                                                        all_data.profile == "VOL")]
                        else:
                            all_data = all_data[
                                (all_data.profile == "HIGH") | (all_data.profile == "LOW") | (
                                            all_data.profile == "SETTLE") | (
                                        all_data.profile == "OPEN") | (
                                        all_data.profile == "CLOSE") | (all_data.profile == "VOL") | (
                                        all_data.profile == "OI")]

                        # print(all_data)

                        # print(oi_data)
                df = df.append(settle_data)
                df = df.append(all_data)
                df = df.append(oi_data)
                df.to_csv(
                    t_path+"temp_files/output_$" + dg_full_product_code + ".csv",
                    mode='a')
                # print(all_data)

                #         with open( root+"/"+file) as s:
                #             reader = csv.reader(s)
                #             for r  in reader:
                #                 list.append(r)

        # print(list)

    def check_spike(self):
        print(settle_low,settle_high)
        for root, dir, files in os.walk("temp_files/"):
            for file in files:

                if dg_full_product_code + ".csv" == file.split("$")[1]:
                    print(file)
                    file = open("temp_files/" + file, 'r')
                    temp_file = csv.reader(file)
                    for temp in temp_file:
                        try:
                            if "SETTLE" == temp[2]:
                                print(temp[3])
                                print( float(temp[3]) , float(200) , float(temp[3]) , float(settle_low))
                                if  float(temp[3]) < float(settle_high) and float(temp[3]) > float(settle_low):
                                    pass
                                else:
                                    row_out_of_spike = [temp_date, temp[1], "SETTLE", temp[3], settle_low, settle_high,
                                                        "SPIKE ISSUE",
                                                        dg_full_product_code, ]
                                    print((row_out_of_spike))
                                    df1_list.append(row_out_of_spike)
                        except:
                            pass

    def html_maker(self):
        global strTable

        strTable = "<html><table class= ='table123'>"

        for num in warning:
            strRW = "<tr class='tr1'><td>" + str(num) + "</td></tr>"
            strTable = strTable + strRW

        strTable = strTable + "</table></html>"

        hs = open("asciiCharHTMLTable.html", 'w')
        hs.write(strTable)

    def email(self):

        global SPIKE1
        emailfrom = "gdmnotification@datagenicgroup.com"
        emailto = "anupam.soni@datagenicgroup.com"
        fileToSend = t_path +'reports/REPORT_test1.xlsx'
        username = "gdmnotification@datagenicgroup.com"
        password = "TAsPaH8y58EThunY"


        if len(df1_list) > 0:
            email_content ="""

                            <html>
                            <head>
                            <style> 
                            table {
                                    border-collapse: collapse;
                                    width: 100%;
                                  }

                            th, td {
                                    text-align: left;
                                    padding: 8px;
                                  }

                                   tr:nth-child(even){background-color: #f2f2f2}            
                            body {
                               font-weight: normal; 

                               background-image: url("");
                               background-color: #fff
                               background-repeat: no-repeat;
                                background-attachment: fixed;
                                background-position:; 
                            }
                            .table123 {
                                   font-weight: normal; 
                                  border-collapse: collapse;
                                border-collapse: collapse;
                                width: 30%;
                            }

                            td, th {
                                border: 2px solid #dddddd;
                                text-align: left;
                                padding: 8px;
                            }

                            tr:nth-child(even) {
                            }
                            </style>
                            </head>
                            <body>
                            <p>&nbsp;</p>
                            <p style="font-size: 1.5em;"><strong>Warning : Data Affacted by Spike </strong>&nbsp;</p>
                            """ + strTable + """
                            <p><strong>Please find the result sheet in email attachment.</strong></p>
                                                    <h2><img src="https://images.g2crowd.com/uploads/product/image/large_detail/large_detail_1489708419/datagenic-genic-datamanager.gif" width="330" height="80" /></h2>

                            </body>
                            </html>
                      """

        elif len(df1_list) < 1:
            email_content =  """

                                        <html>
                                        <head>
                                        <style> 
                                        table {
                                                border-collapse: collapse;
                                                width: 100%;
                                              }

                                        th, td {
                                                text-align: left;
                                                padding: 8px;
                                              }

                                               tr:nth-child(even){background-color: #f2f2f2}            
                                        body {
                                           font-weight: normal; 

                                           background-image: url("");
                                           background-color: #fff
                                           background-repeat: no-repeat;
                                            background-attachment: fixed;
                                            background-position:; 
                                        }
                                        .table123 {
                                               font-weight: normal; 
                                              border-collapse: collapse;
                                            border-collapse: collapse;
                                            width: 30%;
                                        }

                                        td, th {
                                            border: 2px solid #dddddd;
                                            text-align: left;
                                            padding: 8px;
                                        }

                                        tr:nth-child(even) {
                                        }
                                        </style>
                                        </head>
                                        <body>
                                        <p>&nbsp;</p>
                                        <p style="font-size: 1.5em;"><strong>No model present having spike issue : </strong>&nbsp;</p>
                                        <p style="font-size: 1.5em;"><strong>Warning : </strong>&nbsp;</p>

                                        <p><strong> """ + strTable + """</strong></p>
                                                                <h2><img src="https://images.g2crowd.com/uploads/product/image/large_detail/large_detail_1489708419/datagenic-genic-datamanager.gif" width="330" height="80" /></h2>

                                        </body>
                                        </html>
                                  """
            #######################################################################

        msg = MIMEMultipart()
        msg["From"] = emailfrom
        msg["To"] = emailto
        msg["Subject"] = "DAILY SPIKE CHECKS "
        msg.preamble = "CME DAILY CHECKS"
        textual_message = MIMEMultipart('alternative')
        html_part = MIMEText(email_content, 'html')
        textual_message.attach(html_part)
        msg.attach(textual_message)

        #######################################################################
        ctype, encoding = mimetypes.guess_type(fileToSend)
        print(encoding)
        if ctype is None or encoding is not None:
            ctype = "application/octet-stream"

        maintype, subtype = ctype.split("/", 1)
        #######################################################################
        if maintype == "text":
            fp = open(fileToSend, encoding="utf8")
            # Note: we should handle calculating the charset
            attachment = MIMEText(fp.read(), _subtype=subtype)
            fp.close()
        elif maintype == "image":
            fp = open(fileToSend, "rb")
            attachment = MIMEImage(fp.read(), _subtype=subtype)
            fp.close()
        elif maintype == "audio":
            fp = open(fileToSend, "rb")
            attachment = MIMEAudio(fp.read(), _subtype=subtype)
            fp.close()
        else:
            fp = open(t_path +"reports/REPORT_test1.xlsx", "rb")
            attachment = MIMEBase(maintype, subtype)
            attachment.set_payload(fp.read())
            fp.close()
            encoders.encode_base64(attachment)
        if len(df1_list) > 0:
            attachment.add_header("Content-Disposition", "attachment", filename=fileToSend)
            msg.attach(attachment)

        ##################################################################################
        server = smtplib.SMTP("smtp.office365.com:587")
        server.starttls()
        server.login(username, password)
        server.sendmail(emailfrom, emailto, msg.as_string())
        server.quit()
        print("\n        Check you inbox, the result has been sent your email : " + emailto + "\n")

    def drop_files(self):
        files = glob.glob('output/*')
        for f in files:
            os.remove(f)
        files = glob.glob('temp_files/*')
        for f in files:
            os.remove(f)
        files = glob.glob('tempresult/*')
        for f in files:
            os.remove(f)

dbObj = MVvsGDM("Connect MS SQL")
import datetime

dbObj.make_global()
print(warning)
file = pd.read_csv(t_path + "Config.csv", encoding="ISO-8859-1")

file_check = file[file.Action.isin(["CHECK", "Check", "check"])]
isconfig_valid = 1

# print(file_check)
for index, row in file_check.iterrows():
    if (row["SETTLE_LOW"] <row["SETTLE_HIGH"]) :
     pass
    else:
        error_message = "Issue found in high low spike columns."
        warning.append(error_message)
        print(error_message)
if int((str(file.shape)).split(", ")[1][:2]) == 15:
    print((str(file.shape)).split(", ")[1][:2])
else:
    isconfig_valid =0
    print("Header Issue")

# 2

for index, row in file_check.iterrows():
    if row["DG_PRODUCTCD"].startswith("CME_NYMEX"):
        pass
    else:
        isconfig_valid =0
        print("dataset name issue at " + str(index + 2))
#
file_DG_PRODUCTCD = file_check["DG_PRODUCTCD"]
print((file_DG_PRODUCTCD.count()))
file_DG_PRODUCTDESC = file_check["DG_PRODUCTDESC"]
file_SETTLE_LOW = file_check["SETTLE_LOW"]
file_SETTLE_HIGH = file_check["SETTLE_HIGH"]
file_DG_PRODUCTCD_ = file_check["DG_PRODUCTCD_"]
file_RELATIVE_CODE = file_check["RELATIVE_CODE"]

if file_DG_PRODUCTCD.count() == file_DG_PRODUCTDESC.count():
    pass
else:
    isconfig_valid = 0
    print("Somewhere Product description is missing in config file")

if file_DG_PRODUCTCD.count() == file_SETTLE_LOW.count():
    pass
elif file_SETTLE_LOW.count() == file_SETTLE_HIGH.count() and file_DG_PRODUCTCD.count() != file_SETTLE_LOW.count():
    isconfig_valid = 0
    print("Somewhere product settle low and high both are missing in config file")

if file_DG_PRODUCTCD.count() == file_DG_PRODUCTCD_.count():
    pass
else:
    isconfig_valid = 0
    print("issue")

if file_SETTLE_LOW.count() == file_SETTLE_HIGH.count():
    pass
elif file_SETTLE_LOW.count() > file_SETTLE_HIGH.count():
    isconfig_valid = 0
    print("Somewhere settle high is missing in config file")
elif file_SETTLE_LOW.count() < file_SETTLE_HIGH.count():
    isconfig_valid = 0
    print("Somewhere settle low is missing in config file")



if isconfig_valid == 1:
    print("Confing file validated ")
    # current_hour = int(datetime.datetime.now(tz).hour)
    current_hour = 2
    print(current_hour)
    # global warning
    # warning = ""
    file = open(t_path + "Config.csv", 'r')
    cfg_reader = csv.reader(file)
    last_weekday = date.today()
    temp_date = last_weekday.strftime('%Y%m%d')
    if current_hour > 1 and current_hour < 6:
        file_to_read = "ESET"
    elif current_hour > 6 and current_hour < 24:
        file_to_read = "EOD"
    else:
        file_to_read = "OI"
    for c in cfg_reader:
        if "check" in c[1] or "Check" in c[1] or "CHECK" in c[1] or "1" in c[1]:
            # print(c)

            dg_full_product_code = c[0]
            action = c[1]
            dg_product_description = c[2]
            mv_product_code = c[3]
            mv_globex_product_code = c[4]
            current_period = c[5]
            settle_low = c[6]
            settle_high = c[7]
            print(settle_low,settle_high)
            if float(settle_high) < float(settle_low):

                settle_temp = settle_high
                settle_high =settle_low
                settle_low = settle_temp
            print(settle_low,settle_high)

            dg_product_code = c[9]
            bracket = c[10]
            sample_mc_model_code = c[11]
            validation_list.append(dg_full_product_code)
            print("Working on : " + mv_product_code, settle_high, settle_low)
            dbObj.gdm()
            dbObj.MV()
            dbObj.check_spike()
            if today_ in bd_list:
                print("Yes" + str(spike_issue_found))
    # dbObj.check_spike()
    try:
        result_df_1 = pd.DataFrame(df1_list,
                                   columns=['DATE', 'DG MODEL CODE', 'PROFILE', 'VALUE', 'SETTLE HIGH', 'SETTLE LOW',
                                            'ISSUE', 'DATASET CODE'])
    except:
        pass
    try:
        result_df_2 = pd.DataFrame(warning,
                                   columns=['Warnings'])
    except:
        pass
    try:
        result_df_3 = pd.DataFrame(df3_list,
                                   columns=['MV MODEL DESCRIPTION', 'DG MODEL CODE ',
                                            'DG MODEL DESCRIPTION',
                                            'PROFILE', 'MV VALUE', 'DG VALUE', 'OBSERVATION', 'ACTION', 'DG CODE',
                                            'MV CODE'])
    except:
        pass
    try:
        result_df_4 = pd.DataFrame(df4_list,
                                   columns=['MV MODEL DESCRIPTION', 'DG MODEL CODE ',
                                            'DG MODEL DESCRIPTION',
                                            'PROFILE', 'MV VALUE', 'DG VALUE', 'OBSERVATION', 'ACTION', 'DG CODE',
                                            'MV CODE'])
    except:
        pass
    dbObj.make_result()
    dbObj.merger()
    dbObj.html_maker()
    print(len(df1_list))
    print(strTable)
    dbObj.email()
    for r in final_list:
        print(r)
    dbObj.drop_files()
    #


