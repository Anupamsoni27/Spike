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

from datetime import datetime, date, timedelta
from pytz import timezone

now_utc = datetime.now(timezone('UTC'))
now_dublin = now_utc.astimezone(timezone('Europe/Dublin'))
yesterday_utc = datetime.now(timezone('UTC')) - timedelta(1)

server1 = "//10.0.9.58/gdmx/loaderGdmx/"
server2 = "//10.0.9.64/gdmx/loaderGdmx/"
server3 = "//10.0.9.91/gdmx/loaderGdmx/"
test_server = "//10.0.9.61/gdmx/loaderGdmx/"
t_path = "C:\\Users\\anupam.soni\\PycharmProjects\\Spike\\"
# t_path = "C://python_tool//testing//spike//"


class MVvsGDM:
    def __init__(self, name):
        self.name = name

    def make_result(self):
        try:
            print("Running make_result")
            result_df_1.to_csv(t_path + "tempresult/Spike.csv", index=False)
            result_df_2.to_csv(t_path + "tempresult/Warning.csv", index=False)
            # result_df_3.to_csv("tempresult/NOISSUE.csv", index=False)
            # result_df_4.to_csv("tempresult/MISSING.csv", index=False)
        except:
            pass

        try:
            print("Running make_result")
            files_expacted_df.to_csv(t_path + "checked/1. Expected.csv", index=False)
            files_checked_df.to_csv(t_path + "checked/2. Checked_GDMX.csv", index=False)
            # result_df_3.to_csv("tempresult/NOISSUE.csv", index=False)
            # result_df_4.to_csv("tempresult/MISSING.csv", index=False)
        except:
            pass

    def merger(self):
        print("Running merger")
        try:

            wb = xlwt.Workbook()
            for filename in glob.glob(t_path + "tempresult/*.csv"):
                (f_path, f_name) = os.path.split(filename)
                (f_short_name, f_extension) = os.path.splitext(f_name)
                ws = wb.add_sheet(f_short_name)
                spamReader = csv.reader(open(filename, 'rt'))
                for rowx, row in enumerate(spamReader):
                    for colx, value in enumerate(row):
                        ws.write(rowx, colx, value)
            wb.save(t_path + "compiled.xls")
            p.save_book_as(file_name=t_path + 'compiled.xls',
                           dest_file_name=t_path + 'reports/REPORT_test1.xlsx')
        except:
            pass

        try:

            wb = xlwt.Workbook()
            for filename in glob.glob(t_path + "checked/*.csv"):
                (f_path, f_name) = os.path.split(filename)
                (f_short_name, f_extension) = os.path.splitext(f_name)
                ws = wb.add_sheet(f_short_name)
                spamReader = csv.reader(open(filename, 'rt'))
                for rowx, row in enumerate(spamReader):
                    for colx, value in enumerate(row):
                        ws.write(rowx, colx, value)
            wb.save(t_path + "checked.xls")
            p.save_book_as(file_name=t_path + 'checked.xls',
                           dest_file_name=t_path + 'reports/checked.xlsx')
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
        global files_checked_df
        global files_expacted_df

        global flies_checked
        flies_checked = []
        result_df_1 = pd.DataFrame(
            columns=["MV MODEL DESCRIPTION", "DG MODEL CODE ", "DG MODEL DESCRIPTION", "PROFILE", "MV VALUE",
                     "DG VALUE", "OBSERVATION", "ACTION","GDMX NAME","SERVER"])  # MISSMATCH
        result_df_2 = pd.DataFrame(
            columns=["MV MODEL DESCRIPTION", "DG MODEL CODE ", "DG MODEL DESCRIPTION", "PROFILE", "MV VALUE",
                     "DG VALUE", "OBSERVATION", "ACTION"])  # SPIKEISSUE
        result_df_3 = pd.DataFrame(
            columns=["MV MODEL DESCRIPTION", "DG MODEL CODE ", "DG MODEL DESCRIPTION", "PROFILE", "MV VALUE",
                     "DG VALUE", "OBSERVATION", "ACTION"])  # FORALLINFO
        result_df_4 = pd.DataFrame(
            columns=["MV MODEL DESCRIPTION", "DG MODEL CODE ", "DG MODEL DESCRIPTION", "PROFILE", "MV VALUE",
                     "DG VALUE", "OBSERVATION", "ACTION"])  # MISSING
        files_checked_df = pd.DataFrame(columns=["FILES CHECKED","SERVER"])
        files_expacted_df = pd.DataFrame(columns=["FILES EXPECTED OF CATEGORY"])
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

        global files_checked

        global files_expacted

        print("Running gdm")

        global file_to_read
        global status
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
        servers = [server1, server2, server3]
        # servers = ["testing/files"]
        for server in servers:
            print("searching for date : " + temp_date + " DataSet : " + dg_full_product_code + " in Server : " + server)
            files_expacted.append(dg_full_product_code)
            print("AAAAAAAAAAAAAAAAAAA  " + dg_full_product_code)
            for root, dir, files in os.walk(server + dg_full_product_code):
                print(root, files)
                for file in files:
                    print(file)
                    print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$4")

                    # print("searching for "+ temp_date)
                    if check_STL == 1:
                        print("@@@@@@@@@@@@@@@@@@@@@@@@@@@")
                        print(temp_date, dg_full_product_code)
                        if temp_date in file and "ESET" not in file and "EOD" not in file and "OI" not in file and dg_full_product_code in file:
                            cheched_row = file ,server
                            files_checked.append(cheched_row)
                            status = "_STL_"
                            dg_file_present_count = 1
                            file_list.append(file)
                            data = []
                            print("#############################")
                            print(file)
                            print("#############################")
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
                                                                        'value'], model_description, dg_full_product_code, temp_date, dg_product_code, mv_product_code,str(file),server
                                                                    print(row)
                                                                    data.append(row)
                                                                    df = pd.DataFrame(data)

                                df.to_csv(
                                    t_path + "output/output_" + temp_date + "_" + status + "_$" + dg_full_product_code + ".csv",
                                    index=False)
                            except:
                                print("file not found in " + server)
                                pass
                    else:
                        print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$4")
                        if temp_date in file and file_to_read in file and dg_full_product_code in file:

                            if "EOD" in file and file_to_read == "EOD":
                                print(file_to_read)
                                status = "_P_"
                                dg_file_present_count = 1
                            elif "ESET" in file and file_to_read == "ESET":
                                print(file_to_read)

                                status = "_E_"
                                dg_file_present_count = 1
                            # elif "OI" in file and file_to_read == "OI":
                            #     print(file_to_read)
                            #
                            #     status = "_F_"
                            #     dg_file_present_count = 1
                            cheched_row = file, server
                            files_checked.append(cheched_row)
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
                                                                        'value'], model_description, dg_full_product_code, temp_date, dg_product_code,str(file),server
                                                                    data.append(row)
                                                                    df = pd.DataFrame(data)

                                df.to_csv(
                                    t_path + "output/output_" + temp_date + "_" + status + "_$" + dg_full_product_code + ".csv",
                                    index=False)
                            except:
                                print("file not found in " + server)
                                pass
        if dg_file_present_count == 1:
            print(file_to_read + " Files found in server " + server + " for " + dg_full_product_code)
        elif dg_file_present_count == 0:
            print(file_to_read + " file not present for DataSet : " + dg_full_product_code + " date : " + temp_date)
            # warning.append(file_to_read + " file not present for DataSet : " + dg_full_product_code + " date : " + temp_date)
            dg_process_signal = 1
        print("dg_file_present_count " + str(dg_file_present_count))
        print("dg_process_signal " + str(dg_process_signal))
        files_expacted = list(set(files_expacted))
        files_checked = list(set(files_checked))

    def MV(self):
        print("")

        for root, dir, files in os.walk(t_path + "output"):
            # print(root)
            # print("DG FULL PRO: "+ dg_full_product_code )
            # for file in files:
            #     if status in file and dg_full_product_code in file:
            #         with open(root + "/" + file, 'r') as s:
            #             reader = csv.reader(s)
            #             model_list = []
            #             mv_model_list = []
            #             i = 0
            #
            #             for r in reader:
            #                 # print(r)
            #                 if "REGULAR" in r[0] and ".M" in r[0]:
            #                     model_list.append(r[0][-3:])
            #
            #                     # print(r[0][-3:])
            #             # print(len(model_list))
            #             model_list = set(model_list)
            #             model_list = list(model_list)
            #             model_list.sort()
            #             if "YES" in bracket or "yes" in bracket or "Yes" in bracket:
            #                 for model in model_list:
            #                     # print(model)
            #                     if len(str(i)) == 1:
            #                         str_i = "0" + str(i)
            #                     else:
            #                         str_i = str(i)
            #
            #                     mv_model_list.append(mv_product_code + "[" + str_i + "]")
            #                     # print(mv_product_code + "[" + str_i + "]")
            #                     i = i + 1
            #             elif "No" in bracket or "no" in bracket or "NO" in bracket:
            #                 for model in model_list:
            #                     if len(str(i)) == 1:
            #                         str_i = "0" + str(i)
            #                     else:
            #                         str_i = str(i)
            #                     # print(model)
            #                     mv_model_list.append(mv_product_code + str_i)
            #                     # print(mv_product_code + str_i )
            #                     i = i + 1

            # if dg_process_signal == 1:
            df = pd.DataFrame()
            settle_data = pd.DataFrame()
            all_data = pd.DataFrame()
            oi_data = pd.DataFrame()
            for root, dir, files in os.walk(t_path + "output"):
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
                        settle_data["GDMX NAME"] = df1[["7"]]
                        settle_data["SERVER"] = df1[["8"]]
                        # settle_data["MV_PRODUCT_CODE"] = df1[["7"]]

                        # print(settle_data)
                    elif "__STL__$" in file and dg_full_product_code + ".csv" == file.split("$")[1]:
                        print("#####################################################################")
                        count = 1
                        df3 = pd.read_csv(root + "/" + file)
                        oi_data["model"] = df3[["0"]]
                        oi_data["profile"] = df3[["1"]]
                        oi_data["values"] = df3[["2"]]
                        oi_data["model description"] = df3[["3"]]
                        oi_data["DG_FULL_PRODUCT_CODE"] = df3[["4"]]
                        oi_data["DATE"] = df3[["5"]]
                        oi_data["DG_PRODUCT_CODE"] = df3[["6"]]
                        oi_data["GDMX NAME"] = df3[["7"]]
                        oi_data["SERVER"] = df3[["8"]]
                        # oi_data["MV_PRODUCT_CODE"] = df3[["7"]]

                        # oi_data = oi_data[
                        #     (oi_data.profile == "OI")]
                    elif "__P__$" in file and dg_full_product_code + ".csv" == file.split("$")[1]:

                        df2 = pd.read_csv(root + "/" + file)
                        all_data["model"] = df2[["0"]]
                        all_data["profile"] = df2[["1"]]
                        all_data["values"] = df2[["2"]]
                        all_data["model description"] = df2[["3"]]
                        all_data["DG_FULL_PRODUCT_CODE"] = df2[["4"]]
                        all_data["DATE"] = df2[["5"]]
                        all_data["DG_PRODUCT_CODE"] = df2[["6"]]
                        all_data["GDMX NAME"] = df2[["7"]]
                        all_data["SERVER"] = df2[["8"]]
                        # all_data["MV_PRODUCT_CODE"] = df2[["7"]]

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
                print(df.head())
                df.to_csv(
                    t_path + "temp_files/output_$" + dg_full_product_code + ".csv",
                    mode='a')
                # print(all_data)

                #         with open( root+"/"+file) as s:
                #             reader = csv.reader(s)
                #             for r  in reader:
                #                 list.append(r)

        # print(list)

    def check_spike(self):
        print(settle_low, settle_high)
        for root, dir, files in os.walk(t_path + "temp_files/"):
            for file in files:

                if dg_full_product_code + ".csv" == file.split("$")[1]:
                    print(file)
                    file = open(t_path + "temp_files/" + file, 'r')
                    temp_file = csv.reader(file)
                    for temp in temp_file:
                        try:
                            if "SETTLE" == temp[2]:
                                print(temp[3])
                                print(float(temp[3]), float(200), float(temp[3]), float(settle_low))
                                if float(temp[3]) < float(settle_high) and float(temp[3]) > float(settle_low):
                                    pass
                                else:
                                    row_out_of_spike = [temp_date, temp[1], "SETTLE", temp[3], settle_low, settle_high,
                                                        "SPIKE ISSUE",
                                                        dg_full_product_code,temp[8],temp[9]]
                                    print((row_out_of_spike))
                                    df1_list.append(row_out_of_spike)
                        except:
                            pass

    def html_maker(self):
        global strTable

        strTable = "<html><table class='table2' cellpadding='7'><tr><td width='75%' align='left' bgcolor='#eeeeee' style='font-family: Open Sans, Helvetica, Arial, sans-serif; font-size: 16px; font-weight: 800; line-height: 24px; padding: 10px;'>Warnings</td>"

        for num in warning:
            strRW = "<tr class='tr1'><td>" + str(num) + "</td></tr>"
            strTable = strTable + strRW

        strTable = strTable + "</table></html>"

        hs = open("asciiCharHTMLTable.html", 'w')
        hs.write(strTable)

        global strTable3

        strTable3 = "<html><table class='table123'>"
        if len(df1_list) > 0:
            strTable3 = strTable3 + "<tr><th>Dataset Code</th><th>Model Code</th><th>Settle with Spike</th><th>Expected Settle High</th><th>Expected Settle Low</th></tr>"
        else:
            strTable3 = strTable3 + "<h3>None</h3>"
        for num3 in df1_list:
            strRW3 = "<tr><td>" + str(num3[7]) + "</td><td>" + str(num3[1]) + "</td><td>" + str(
                num3[3]) + "</td><td>" + str(num3[4]) + "</td><td>" + str(num3[5]) + "</td></tr>"
            strTable3 = strTable3 + strRW3

        strTable3 = strTable3 + "</table></html>"

        hs3 = open("asciiCharHTMLTable.html", 'w')
        hs3.write(strTable3)
        print(strTable3)

    def email_result_status(self):

        global SPIKE1
        emailfrom = "gdmnotification@datagenicgroup.com"
        emailto = "dgsupport@datagenicgroup.com"
        fileToSend = t_path + 'reports/REPORT_test1.xlsx'
        username = "gdmnotification@datagenicgroup.com"
        password = "TAsPaH8y58EThunY"
        email_content = """"""
        if len(df1_list) > 0:
            email_content = """
                                <!DOCTYPE html>
        <html>
        <head>
        <title></title>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <meta http-equiv="X-UA-Compatible" content="IE=edge" />
        <style type="text/css">





                                        .table123 {
        font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
        border-collapse: collapse;
        width: 100%;
    }
            .table2 td {
                  border: 1px solid #ddd;
                  width: 800px
            }
    .nowrap {
        white-space: nowrap;
    }


    .table123  td, .table123  th {
        border: 1px solid #ddd;
        padding: 8px;
    }
    border: 1px solid #ddd;.table2 {

    }
    .table123  tr:nth-child(even){background-color: #f2f2f2;}

    .table123  tr:hover {background-color: #ddd;}

    .table123  th {
        padding-top: 12px;
        padding-bottom: 12px;
        text-align: left;
        background-color: #ddd;
        color: black;

    }

        /* CLIENT-SPECIFIC STYLES */
        body, table, td, a { -webkit-text-size-adjust: 100%; -ms-text-size-adjust: 100%; }
        table, td { mso-table-lspace: 0pt; mso-table-rspace: 0pt; }
        img { -ms-interpolation-mode: bicubic; }

        /* RESET STYLES */
        img { border: 0; height: auto; line-height: 100%; outline: none; text-decoration: none; }
        table { border-collapse: collapse !important; }
        body { height: 100% !important; margin: 0 !important; padding: 0 !important; width: 100% !important; }

        /* iOS BLUE LINKS */
        a[x-apple-data-detectors] {
            color: inherit !important;
            text-decoration: none !important;
            font-size: inherit !important;
            font-family: inherit !important;
            font-weight: inherit !important;
            line-height: inherit !important;
        }

        /* MEDIA QUERIES */
        @media screen and (max-width: 480px) {
            .mobile-hide {
                display: none !important;
            }
            .mobile-center {
                text-align: center !important;
            }
        }

        /* ANDROID CENTER FIX */
        div[style*="margin: 16px 0;"] { margin: 0 !important; }
        </style>
        <body style="margin: 0 !important; padding: 0 !important; background-color: #eeeeee;" bgcolor="#eeeeee">

        <!-- HIDDEN PREHEADER TEXT -->
        <div style="display: none; font-size: 1px; color: #fefefe; line-height: 1px; font-family: Open Sans, Helvetica, Arial, sans-serif; max-height: 0px; max-width: 0px; opacity: 0; overflow: hidden;">

        </div>

        <table border="0" cellpadding="0" cellspacing="0" width="100%">
            <tr>
                <td align="center" style="background-color: #eeeeee;" bgcolor="#eeeeee">
                <!--[if (gte mso 9)|(IE)]>
                <table align="center" border="0" cellspacing="0" cellpadding="0" width="600">
                <tr>
                <td align="center" valign="top" width="600">
                <![endif]-->
                <table align="center" border="0" cellpadding="0" cellspacing="0" width="100%" style="max-width:600px;">
                    <tr>

                    </tr>
                    <tr>
                        <td align="center" style=" padding: 35px; background-color: #ffffff;" bgcolor="#ffffff">
                        <!--[if (gte mso 9)|(IE)]>
                        <table align="center" border="0" cellspacing="0" cellpadding="0" width="600">
                        <tr>
                        <td align="center" valign="top" width="600">
                        <![endif]-->
                        <table align="center" border="0" cellpadding="0" cellspacing="0" width="100%" style="max-width:600px;">
                            <tr>
                                <td align="center" style="font-family: Open Sans, Helvetica, Arial, sans-serif; font-size: 16px; font-weight: 400; line-height: 24px; padding-top: 25px;">
                                   <h2 style="font-size: 24px; font-weight: 800; line-height: 30px; color: #000; margin: 0;">
                                        <u>Failure Details</u> 
                                    </h2>  
                                </td>
                            </tr>
                            <tr>
                                <td align="center" style="padding: 25px 0 15px 0;">
                                    <table border="0" cellspacing="0" cellpadding="0">
                                        <tr>
                                            <td align="center" style="border-radius: 5px;" bgcolor="#66b3b7">
                                           <!--   <a href="http://litmus.com" target="_blank" style="font-size: 18px; font-family: Open Sans, Helvetica, Arial, sans-serif; color: #ffffff; text-decoration: none; border-radius: 5px; background-color: #66b3b7; padding: 15px 30px; border: 1px solid #66b3b7; display: block;">Awesome</a>
                                            </td>
                                        </tr>
                                    </table>
                                </td>
                            </tr>
                        </table>
                        <!--[if (gte mso 9)|(IE)]>
                        </td>
                        </tr>
                        </table>
                        <![endif]-->
                        </td>
                    </tr>
                    <tr>
                        <td align="center" style="padding: 5px; background-color: #ffffff;" bgcolor="#ffffff">
                        <!--[if (gte mso 9)|(IE)]>
                        <table align="center" border="0" cellspacing="0" cellpadding="0" width="600">
                        <tr>
                        <td align="center" valign="top" width="600">
                        <![endif]-->

                        <!--[if (gte mso 9)|(IE)]>
                        </td>
                        </tr>
                        </table>
                        <![endif]-->""" + strTable3 + """<br><p></p>

                        </td>
                    </tr>

                </table>
                <!--[if (gte mso 9)|(IE)]>
                </td>
                </tr>
                </table>
                <![endif]-->
                </td>
            </tr>
        </table>
                        </div>
                        <!--[if (gte mso 9)|(IE)]>
                        </td>
                        </tr>
                        </table>
                        <![endif]-->
                        </td>
                    </tr>
                    <tr>
                        <td align="center" style="padding: 0px 35px 20px 35px; background-color: #ffffff;" bgcolor="#ffffff">
                        <!--[if (gte mso 9)|(IE)]>
                        <table align="center" border="0" cellspacing="0" cellpadding="0" width="600">
                        <tr>
                        <td align="center" valign="top" width="600">
                        <![endif]-->





                        <table align="center" border="0" cellpadding="0" cellspacing="0" width="100%" style="max-width:600px;">

                                    <table  class="table2" cellspacing="0" cellpadding="0" border="0" width="100%">


                                            </td>
                                        </tr>
                                    </table>
                                </td>
                            </tr>
                            <tr>
                                <td align="left" style="padding-top: 20px;">
                                    <table cellspacing="0" cellpadding="0" border="0" width="100%">

                                    </table>
                                </td>
                            </tr>
                        </table>
                        <!--[if (gte mso 9)|(IE)]>
                        </td>
                        </tr>
                        </table>
                        <![endif]-->
                        </td>
                    </tr>
                     <tr>
                        <td align="center" height="100%" valign="top" width="100%" style="padding: 0 35px 35px 35px; background-color: #ffffff;" bgcolor="#ffffff"><u>Validated for the Observation index : """ + str(
                yesterday_utc.strftime("%Y-%m-%d")) + """</u>
                        <!--[if (gte mso 9)|(IE)]>
                        <table align="center" border="0" cellspacing="0" cellpadding="0" width="600">
                        <tr>
                        <td align="center" valign="top" width="600">
                        <![endif]-->



        </body>
        </html>

                          """

        elif len(df1_list) < 1:
            email_content = """"""

            #######################################################################

        msg = MIMEMultipart()
        msg["From"] = emailfrom
        msg["To"] = emailto
        if len(df1_list) > 0:
            msg["Subject"] = "CME " + email_status + " GDMX Spike Checks – Failure – " + now_dublin.strftime(
                "%Y-%m-%d %H:%M") + ""
        elif len(df1_list) < 1:
            msg["Subject"] = "CME " + email_status + " GDMX Spike Checks – Success – " + now_dublin.strftime(
                "%Y-%m-%d %H:%M") + ""
        msg.preamble = "CME  DAILY CHECKS"
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
            fp = open(t_path + "reports/REPORT_test1.xlsx", "rb")
            attachment = MIMEBase(maintype, subtype)
            attachment.set_payload(fp.read())
            fp.close()
            encoders.encode_base64(attachment)

        if len(df1_list) > 0:
            attachment.add_header("Content-Disposition", "attachment",
                                  filename="Report_" + str(now_dublin.strftime("%Y%m%d_%H%M")) + ".xlsx")
            msg.attach(attachment)
        fp = open(t_path + "reports/checked.xlsx", "rb")
        attachment = MIMEBase(maintype, subtype)
        attachment.set_payload(fp.read())
        fp.close()
        encoders.encode_base64(attachment)
        attachment.add_header("Content-Disposition", "attachment",
                              filename="Spike Checked Info -" + str(now_dublin.strftime("%Y%m%d-%H:%M")) + ".xlsx")
        msg.attach(attachment)
        ##################################################################################
        server = smtplib.SMTP("smtp.office365.com:587")
        server.starttls()
        server.login(username, password)
        server.sendmail(emailfrom, emailto, msg.as_string())
        server.quit()
        print("\n        Check you inbox, the result has been sent your email : " + emailto + "\n")

    def email_warnings(self):

        global SPIKE1
        emailfrom = "gdmnotification@datagenicgroup.com"
        emailto = "dataqa@datagenicgroup.com"
        fileToSend = t_path + 'reports/REPORT_test1.xlsx'
        username = "gdmnotification@datagenicgroup.com"
        password = "TAsPaH8y58EThunY"
        email_content = """"""

        email_content = """
                                <!DOCTYPE html>
        <html>
        <head>
        <title></title>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <meta http-equiv="X-UA-Compatible" content="IE=edge" />
        <style type="text/css">





                                        .table123 {
        font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
        border-collapse: collapse;
        width: 100%;
    }
            .table2 td {
                  border: 1px solid #ddd;
                  width: 800px
            }
    .nowrap {
        white-space: nowrap;
    }


    .table123  td, .table123  th {
        border: 1px solid #ddd;
        padding: 8px;
    }
    border: 1px solid #ddd;.table2 {

    }
    .table123  tr:nth-child(even){background-color: #f2f2f2;}

    .table123  tr:hover {background-color: #ddd;}

    .table123  th {
        padding-top: 12px;
        padding-bottom: 12px;
        text-align: left;
        background-color: #ddd;
        color: black;

    }

        /* CLIENT-SPECIFIC STYLES */
        body, table, td, a { -webkit-text-size-adjust: 100%; -ms-text-size-adjust: 100%; }
        table, td { mso-table-lspace: 0pt; mso-table-rspace: 0pt; }
        img { -ms-interpolation-mode: bicubic; }

        /* RESET STYLES */
        img { border: 0; height: auto; line-height: 100%; outline: none; text-decoration: none; }
        table { border-collapse: collapse !important; }
        body { height: 100% !important; margin: 0 !important; padding: 0 !important; width: 100% !important; }

        /* iOS BLUE LINKS */
        a[x-apple-data-detectors] {
            color: inherit !important;
            text-decoration: none !important;
            font-size: inherit !important;
            font-family: inherit !important;
            font-weight: inherit !important;
            line-height: inherit !important;
        }

        /* MEDIA QUERIES */
        @media screen and (max-width: 480px) {
            .mobile-hide {
                display: none !important;
            }
            .mobile-center {
                text-align: center !important;
            }
        }

        /* ANDROID CENTER FIX */
        div[style*="margin: 16px 0;"] { margin: 0 !important; }
        </style>
        <body style="margin: 0 !important; padding: 0 !important; background-color: #eeeeee;" bgcolor="#eeeeee">

        <!-- HIDDEN PREHEADER TEXT -->
        <div style="display: none; font-size: 1px; color: #fefefe; line-height: 1px; font-family: Open Sans, Helvetica, Arial, sans-serif; max-height: 0px; max-width: 0px; opacity: 0; overflow: hidden;">

        </div>

        <table border="0" cellpadding="0" cellspacing="0" width="100%">
            <tr>
                <td align="center" style="background-color: #eeeeee;" bgcolor="#eeeeee">
                <!--[if (gte mso 9)|(IE)]>
                <table align="center" border="0" cellspacing="0" cellpadding="0" width="600">
                <tr>
                <td align="center" valign="top" width="600">
                <![endif]-->
                <table align="center" border="0" cellpadding="0" cellspacing="0" width="100%" style="max-width:600px;">
                    <tr>

                    </tr>
                    <tr>
                        <td align="center" style=" padding: 35px; background-color: #ffffff;" bgcolor="#ffffff">
                        <!--[if (gte mso 9)|(IE)]>
                        <table align="center" border="0" cellspacing="0" cellpadding="0" width="600">
                        <tr>
                        <td align="center" valign="top" width="600">
                        <![endif]-->
                        <table align="center" border="0" cellpadding="0" cellspacing="0" width="100%" style="max-width:600px;">
                            <tr>
                                <td align="center" style="font-family: Open Sans, Helvetica, Arial, sans-serif; font-size: 16px; font-weight: 400; line-height: 24px; padding-top: 25px;">
                                   <h2 style="font-size: 24px; font-weight: 800; line-height: 30px; color: #000; margin: 0;">
                                        <!--<u>Warnings</u>--> 
                                    </h2>  
                                </td>
                            </tr>
                            <tr>
                                <td align="center" style="padding: 25px 0 15px 0;">
                                    <table border="0" cellspacing="0" cellpadding="0">
                                        <tr>
                                            <td align="center" style="border-radius: 5px;" bgcolor="#66b3b7">
                                           <!--   <a href="http://litmus.com" target="_blank" style="font-size: 18px; font-family: Open Sans, Helvetica, Arial, sans-serif; color: #ffffff; text-decoration: none; border-radius: 5px; background-color: #66b3b7; padding: 15px 30px; border: 1px solid #66b3b7; display: block;">Awesome</a>
                                            </td>
                                        </tr>
                                    </table>
                                </td>
                            </tr>
                        </table>
                        <!--[if (gte mso 9)|(IE)]>
                        </td>
                        </tr>
                        </table>
                        <![endif]-->
                        </td>
                    </tr>
                    <tr>
                        <td align="center" style="padding: 5px; background-color: #ffffff;" bgcolor="#ffffff">
                        <!--[if (gte mso 9)|(IE)]>
                        <table align="center" border="0" cellspacing="0" cellpadding="0" width="600">
                        <tr>
                        <td align="center" valign="top" width="600">
                        <![endif]-->

                        <!--[if (gte mso 9)|(IE)]>
                        </td>
                        </tr>
                        </table>
                        """ + strTable + """
                        </td>
                    </tr>

                </table>
                <!--[if (gte mso 9)|(IE)]>
                </td>
                </tr>
                </table>
                <![endif]-->
                </td>
            </tr>
        </table>
                        </div>
                        <!--[if (gte mso 9)|(IE)]>
                        </td>
                        </tr>
                        </table>
                        <![endif]-->
                        </td>
                    </tr>
                    <tr>
                        <td align="center" style="padding: 0px 35px 20px 35px; background-color: #ffffff;" bgcolor="#ffffff">
                        <!--[if (gte mso 9)|(IE)]>
                        <table align="center" border="0" cellspacing="0" cellpadding="0" width="600">
                        <tr>
                        <td align="center" valign="top" width="600">
                        <![endif]-->





                        <table align="center" border="0" cellpadding="0" cellspacing="0" width="100%" style="max-width:600px;">

                                    <table  class="table2" cellspacing="0" cellpadding="0" border="0" width="100%">


                                            </td>
                                        </tr>
                                    </table>
                                </td>
                            </tr>
                            <tr>
                                <td align="left" style="padding-top: 20px;">
                                    <table cellspacing="0" cellpadding="0" border="0" width="100%">

                                    </table>
                                </td>
                            </tr>
                        </table>
                        <!--[if (gte mso 9)|(IE)]>
                        </td>
                        </tr>
                        </table>
                        <![endif]-->
                        </td>
                    </tr>
                     <tr>
                        <td align="center" height="100%" valign="top" width="100%" style="padding: 0 35px 35px 35px; background-color: #ffffff;" bgcolor="#ffffff"><u>Validated for the Observation index : """ + str(
            yesterday_utc.strftime("%Y-%m-%d")) + """</u>
                        <!--[if (gte mso 9)|(IE)]>
                        <table align="center" border="0" cellspacing="0" cellpadding="0" width="600">
                        <tr>
                        <td align="center" valign="top" width="600">
                        <![endif]-->



        </body>
        </html>

                          """
        msg = MIMEMultipart()
        msg["From"] = emailfrom
        msg["To"] = emailto
        msg["Subject"] = "CME " + email_status + " GDMX Spike Checks – Warnings – " + now_dublin.strftime(
            "%Y-%m-%d %H:%M") + ""
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
            fp = open(t_path + "reports/REPORT_test1.xlsx", "rb")
            attachment = MIMEBase(maintype, subtype)
            attachment.set_payload(fp.read())
            fp.close()
            encoders.encode_base64(attachment)

        ##################################################################################
        server = smtplib.SMTP("smtp.office365.com:587")
        server.starttls()
        server.login(username, password)
        server.sendmail(emailfrom, emailto, msg.as_string())
        server.quit()
        print("\n        Check you inbox, the result has been sent your email : " + emailto + "\n")

    def drop_files(self):
        files = glob.glob(t_path + 'output/*')
        for f in files:
            os.remove(f)
        files = glob.glob(t_path + 'temp_files/*')
        for f in files:
            os.remove(f)
        files = glob.glob(t_path + 'tempresult/*')
        for f in files:
            os.remove(f)
            files = glob.glob(t_path + 'reports/*')
        for f in files:
            os.remove(f)


dbObj = MVvsGDM("Connect MS SQL")
import datetime

global email_status
global check_STL

check_STL = 0
dbObj.make_global()
print(warning)
file = pd.read_csv(t_path + "Config.csv", encoding="ISO-8859-1")

file_check = file[file.Action.isin(["CHECK", "Check", "check"])]
isconfig_valid = 1

# print(file_check)
for index, row in file_check.iterrows():
    if (row["SETTLE_LOW"] < row["SETTLE_HIGH"]):
        pass
    else:
        error_message = "High & low spike values are swapped in Config file."
        # warning.append(error_message)
        print(error_message)
if int((str(file.shape)).split(", ")[1][:2]) == 17:
    print((str(file.shape)).split(", ")[1][:2])
else:
    isconfig_valid = 0
    print("Header Issue")

# 2

for index, row in file_check.iterrows():
    if row["DG_PRODUCTCD"].startswith("CME_NYMEX"):
        pass
    else:
        isconfig_valid = 0
        print("dataset name issue at " + str(index + 2))
        error_message = "dataset name issue at " + str(index + 2)
        warning.append(error_message)
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
    error_message = "Somewhere Product description is missing in config file"
    warning.append(error_message)

if file_DG_PRODUCTCD.count() == file_SETTLE_LOW.count():
    pass
elif file_SETTLE_LOW.count() == file_SETTLE_HIGH.count() and file_DG_PRODUCTCD.count() != file_SETTLE_LOW.count():
    isconfig_valid = 0
    print("Somewhere product settle low and high both are missing in config file")
    error_message = "Somewhere product settle low and high both are missing in config file"
    warning.append(error_message)

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
    error_message = "Somewhere settle high is missing in config file"
    warning.append(error_message)
elif file_SETTLE_LOW.count() < file_SETTLE_HIGH.count():
    isconfig_valid = 0
    print("Somewhere settle low is missing in config file")
    error_message = "Somewhere settle low is missing in config file"
    warning.append(error_message)
if isconfig_valid == 1:
    print("Confing file validated ")

    from datetime import datetime, date, timedelta
    from pytz import timezone

    now_utc = datetime.now(timezone('UTC'))
    now_dublin = now_utc.astimezone(timezone('Europe/Dublin'))
    print(now_dublin.hour)
    files_checked = []
    files_expacted = []
    # current_hour = int(datetime.datetime.now(tz).hour)
    current_hour = int(str(now_dublin.hour))
    print(current_hour)
    # global warning
    # warning = ""

    file = open(t_path + "Config.csv", 'r')
    cfg_reader = csv.reader(file)
    last_weekday = date.today()
    temp_date = last_weekday.strftime('%Y%m%d')
    # temp_date = "20180627"
    if current_hour > 4 and current_hour < 6:
        file_to_read = "ESET"
        email_status = "E"
    elif current_hour > 6 and current_hour < 15:
        file_to_read = "EOD"
        email_status = "P"
    elif current_hour > 22:
        check_STL = 1
        file_to_read = "STL"
        email_status = "STL"
        print("33333")

    for c in cfg_reader:
        if file_to_read == "STL":
            if "check" in c[1] or "Check" in c[1] or "CHECK" in c[1] or "1" in c[1]:
                if c[15] == "YES":
                    dg_full_product_code = c[0]
                    action = c[1]
                    dg_product_description = c[2]
                    mv_product_code = c[3]
                    mv_globex_product_code = c[4]
                    current_period = c[5]
                    settle_low = c[6]
                    settle_high = c[7]
                    print(settle_low, settle_high)
                    if float(settle_high) < float(settle_low):
                        warning_row = "High & low spike values of " + dg_full_product_code + " are swapped in Config file."
                        warning.append(warning_row)
                        settle_temp = settle_high
                        settle_high = settle_low
                        settle_low = settle_temp
                    print(settle_low, settle_high)

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
        if file_to_read == "ESET":
            if "check" in c[1] or "Check" in c[1] or "CHECK" in c[1] or "1" in c[1]:
                print("###########################3")
                if c[16] == "NO":
                    dg_full_product_code = c[0]
                    action = c[1]
                    dg_product_description = c[2]
                    mv_product_code = c[3]
                    mv_globex_product_code = c[4]
                    current_period = c[5]
                    settle_low = c[6]
                    settle_high = c[7]
                    print(settle_low, settle_high)
                    if float(settle_high) < float(settle_low):
                        warning_row = "High & low spike values of " + dg_full_product_code + " are swapped in Config file."
                        warning.append(warning_row)
                        settle_temp = settle_high
                        settle_high = settle_low
                        settle_low = settle_temp
                    print(settle_low, settle_high)

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
        if file_to_read == "STL":
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
                print(settle_low, settle_high)
                if float(settle_high) < float(settle_low):
                    warning_row = "High & low spike values of " + dg_full_product_code + " are swapped in Config file."
                    warning.append(warning_row)
                    settle_temp = settle_high
                    settle_high = settle_low
                    settle_low = settle_temp
                print(settle_low, settle_high)

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
                                   columns=['DATE', 'DG MODEL CODE', 'PROFILE', 'SETTLE WITH SPIKE', 'SETTLE HIGH', 'SETTLE LOW',
                                            'ISSUE', 'DATASET CODE','GDMX NAME','SERVER'])
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
    try:
        files_checked_df = pd.DataFrame(files_checked,
                                        columns=['FILES CHECKED','SERVER'])
    except:
        pass
    try:
        files_expacted_df = pd.DataFrame(files_expacted,
                                         columns=['FILES EXPECTED OF CATEGORY'])
    except:
        pass
    dbObj.make_result()
    dbObj.merger()
    dbObj.html_maker()
    print(len(df1_list))
    print(strTable)
    #if len(warning) > 0:
        #dbObj.email_warnings()
    #dbObj.email_result_status()
    for r in final_list:
        print(r)
    dbObj.drop_files()



