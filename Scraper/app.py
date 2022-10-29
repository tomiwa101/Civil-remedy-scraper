import urllib
import requests
from scrapy import Selector
import psycopg2
from configparser import ConfigParser


#Reading developer api key to use for proxy
configur = ConfigParser()
configur.read('config.ini')
API_KEY = configur.get('APP','API_KEY')  #Retreiving scraper api key from config file
postgre_pwd = configur.get('APP','postgre_pwd')  #Retreiving postgres db password config file


#Specifying a few parameters
hostname = 'localhost'
database = 'civil_remedy'
username = 'postgres'
pwd = postgre_pwd
port_id = 5432
conn, cur = None, None


# def get_scraperapi_url(url):
#         payload = {'api_key': API_KEY, 'url': url}
#         proxy_url = 'http://api.scraperapi.com/?' + urllib.parse.urlencode(payload)
#         return proxy_url


def get_data(page_no: int):
    url = f'https://apps.fldfs.com/CivilRemedy/ViewFiling.aspx?fid={str(page_no)}'
    # proxy_url = get_scraperapi_url(url)
    data = requests.get(url = url).text
    return data


def check_odd(var_list: list):
    return len(var_list) % 2 == 1


def break_list(var_list: list):
    sol = list()
    odd = check_odd(var_list)
    for i in range(0, len(var_list), 2):
        if odd and i==len(var_list)-1:
            sol.append([var_list[i]])
            continue
        sol.append([var_list[i], var_list[i+1]])
    return sol


# get header details
def get_fil_no(sel):
    try:
        sol = sel.css('span#ctl00_phPageContent_lblFilingAcceptedId::text').get()
    except:
        sol = 'Not found'
    return sol


def get_fil_date(sel):
    try:
        sol = sel.css('span#ctl00_phPageContent_lblFilingDate::text').get()
    except:
        sol = 'Not found'
    return sol


# Get Complainant Section
def get_comp_sect(sel):
    Complainant_list = list()
    for item in sel.xpath("//td[@class='sectionHeader' and contains(./text(), 'Complainant')]/parent::tr/following-sibling::tr/*/*/*").getall():
        new_sel = Selector( text = item )
        value = (new_sel.xpath("//span//text()").getall())
        if len(value) >1:
            value = [inp for inp in value if inp != '*']
        Complainant_list.append(value)
    return Complainant_list


def get_comp_values(comp_list: list):
    return {
        'Complaint_Last_Business_Name': comp_list[0][0],
        'Complaint_First_Name': ' '.join(comp_list[0][1:]),
        'Complaint_Street_Address': ' '.join(comp_list[1]),
        'Complaint_City_State_ZIP': ' '.join(comp_list[2]),
        'Complaint_Email_Address': ' '.join(comp_list[3]).replace('*', ''),
        'Complainant_Type': ' '.join(comp_list[4])
    }


# get Insured Sections
def get_ins_sect(sel):
    insured_list = list()
    for item in sel.xpath("//td[@class='sectionHeader' and contains(./text(), 'Insured')]/parent::tr/following-sibling::tr/*/*/*").getall():
        new_sel = Selector( text = item )
        insured_list.append(new_sel.xpath("//span/text()").getall())
    return insured_list


def get_ins_name(ins_list: list):
    ins_inp1 = ins_list[0]
    if len(ins_inp1) >1:
        ins_inp1 = [inp for inp in ins_inp1 if inp != '*']
    try:
        Insured_Last_Business_Name = ins_inp1[0]
    except:
        Insured_Last_Business_Name = 'Not found'
    try:
        Insured_First_Name = ' '.join(ins_inp1[1:])
    except:
        Insured_First_Name = 'Not found'
    return Insured_Last_Business_Name, Insured_First_Name


def get_ins_no(ins_list: list):
    Insured_Polciy_No = ins_list[1][1].replace('*','')
    Insured_Claim_No = ins_list[1][-1].replace('*','')
    return Insured_Polciy_No, Insured_Claim_No 


# get Attorney Sections
def get_att_sect(sel):
    attorney_list = list()
    for item in sel.xpath("//div[@id = 'ctl00_phPageContent_div_Attorney']/*/*/*").getall():
        new_sel = Selector( text = item )
        if len(new_sel.xpath("//span").getall()) != 0:
            value2 = (new_sel.xpath("//span//text()").getall())
            # if len(value2) >1:
            #     value2 = [inp for inp in value2 if inp != '*']
            attorney_list.append(value2)
    return attorney_list

def get_att_values(att_list: list):
    if len(att_list[0][-1].strip()) > 1:
        initial = ''
    else:
        initial = att_list[0][-1]
    if len(att_list[0][-1].strip()) > 1:
        last_name = att_list[0][-1].replace('*','')
    else:
        last_name = att_list[0][-2].replace('*','')
    return {
        'Attorney_Last_Name': att_list[0][1].replace('*',''),
        'Attorney_First_Name': last_name,
        'Attorney_Initial': initial,
        'Attorney_Street_Address': ' '.join(att_list[2]),
        'Attorney_City_State_Zip': ' '.join(att_list[4]).replace('*', ''),
        'Attorney_Email_Address': ' '.join(att_list[-1])
    }


# get violation section
# get Violation insirer name
def get_vio_ins_no(sel):
    try:
        sol = sel.css('span#ctl00_phPageContent_lblAuthInsurerName::text').get()
    except:
        sol = 'Not found'
    return sol


def get_vio_naic_cc(sel):
    try:
        sol = sel.css('span#ctl00_phPageContent_lblNAICCompCode::text').get().replace('NAIC Company Code', '').strip()
    except:
        sol = 'Not found'
    return sol


def get_other_vio_sect(sel):
    viol_cat = sel.xpath("//tr[@id='ctl00_phPageContent_AuthInsurerNAICCompCodeRow']/following-sibling::tr//text()").getall()
    viol_cat = [cont.replace('\t','').replace('\n','').strip() for cont in viol_cat]
    viol_cat = [n for n in viol_cat if n != '']
    return viol_cat


def get_ind_det_vio(vio_list: list):
    try:
        as_pos = vio_list.index('*')
        tyofinpos = vio_list.index('Type of Insurance')
        if (tyofinpos - as_pos) == 1:
            Individual_Responsible_for_viloation = ''
        else:
            Individual_Responsible_for_viloation = vio_list[2]
    except: 
        Individual_Responsible_for_viloation = 'Not found'
    Violation_Type_of_Insurance = vio_list[-1].replace('*','')
    return Individual_Responsible_for_viloation, Violation_Type_of_Insurance


def get_vio_reason(sel):
    RN_List = sel.xpath("//table[@id ='ctl00_phPageContent_gvReasonsForNotice']//td/text()").getall()
    RN_List = [n.replace('\t','').replace('\n','').replace('\r','') for n in RN_List]
    RN_List = [n for n in RN_List if n != '']
    Vioaltion_Reason_for_Notice = ', '.join(RN_List)
    return Vioaltion_Reason_for_Notice


def get_sp_vio(sel):
    SPIV = sel.xpath("//table[@id='ctl00_phPageContent_gvStatutes']//tr//text()").getall()
    SPIV = [n.replace('\t','').replace('\n','').replace('\r','') for n in SPIV]
    SPIV = [n for n in SPIV if n != '']
    upd_SPIV = break_list(SPIV)
    upd = [':'.join(n) for n in upd_SPIV]
    Statutory_provisions_insurer_violated = ', '.join(upd)
    return Statutory_provisions_insurer_violated


def get_pol_lang_fc(sel):
    Viol_last = sel.xpath("//span[@id = 'ctl00_phPageContent_UpdatePanel1']/parent::td/parent::tr/following-sibling::tr//span/text()").getall()
    Specific_policy_language = Viol_last[1].replace('*','')
    Facts_circumstances_giving_rise_to_the_violation = Viol_last[-1].replace('*','')
    return Specific_policy_language, Facts_circumstances_giving_rise_to_the_violation


def main():
    start_page = 260000
    end_page = 651200
    pages = list(range(start_page, end_page+1))
    try:
        conn = psycopg2.connect(
                host = hostname,
                dbname = database,
                user = username,
                password = pwd,
                port = port_id
        )

        cur = conn.cursor()
        cur = conn.cursor()

        cur.execute('DROP TABLE IF EXISTS civil_remedy_cases')
        create_script = ''' CREATE TABLE IF NOT EXISTS civil_remedy_cases (
                                filing_no int PRIMARY KEY,
                                Filing_date date,
                                Complainant_Last_Business_Name varchar(50),
                                Complainant_First_Name varchar(50),
                                Complainant_Street_Address varchar(100),
                                Complainant_City_State_ZIP varchar(50),
                                Complainant_Email_Address varchar(100),
                                Complainant_Type varchar(100),
                                Insured_Last_Business_Name varchar(50),
                                Insured_First_Name varchar(100),
                                Insured_Polciy_No varchar(100),
                                Insured_Claim_No varchar(100),
                                Attorney_Last_Name varchar(100),
                                Attorney_First_Name varchar(100),
                                Attorney_Initial varchar(50),
                                Attorney_Street_Address varchar(100),
                                Attorney_City_State_Zip varchar(100),
                                Attorney_Email_Address varchar(100),
                                Violation_Insurer_Name varchar(100),
                                Violation_NAIC_Company_Code varchar(100),
                                Individual_Responsible_for_viloation varchar(100),
                                Violation_Type_of_Insurance varchar(100),
                                Violation_Reason_for_Notice varchar(2000),
                                Statutory_provisions_insurer_violated varchar(2000),
                                Specific_policy_language varchar(2000)
        )
        '''

        cur.execute(create_script)

        for page in pages: 
            try:
                html = get_data(page)
                sel = Selector( text = html )

                Filing_no = get_fil_no(sel)
                Filing_date = get_fil_date(sel)

                comp_list = get_comp_sect(sel)
                complaint = get_comp_values(comp_list)

                ins_list = get_ins_sect(sel)
                Insured_Last_Business_Name, Insured_First_Name = get_ins_name(ins_list)
                Insured_Polciy_No, Insured_Claim_No = get_ins_no(ins_list)

                att_list = get_att_sect(sel)
                attorney = get_att_values(att_list)

                Violation_Insurer_Name = get_vio_ins_no(sel)
                Violation_NAIC_Company_Code = get_vio_naic_cc(sel)
                vio_list = get_other_vio_sect(sel)
                Individual_Responsible_for_viloation, Violation_Type_of_Insurance = get_ind_det_vio(vio_list)
                Vioaltion_Reason_for_Notice = get_vio_reason(sel)
                Statutory_provisions_insurer_violated = get_sp_vio(sel)
                Specific_policy_language, Facts_circumstances_giving_rise_to_the_violation = get_pol_lang_fc(sel)

                final_data = {
                    'Filing_no': int(Filing_no),
                    'Filing_date': Filing_date,
                    'Complainant_Last_Business_Name': complaint['Complaint_Last_Business_Name'],
                    'Complainant_First_Name': complaint['Complaint_First_Name'],
                    'Complainant_Street_Address': complaint['Complaint_Street_Address'],
                    'Complainant_City_State_ZIP': complaint['Complaint_City_State_ZIP'],
                    'Complainant_Email_Address': complaint['Complaint_Email_Address'],
                    'Complainant_Type': complaint['Complainant_Type'],
                    'Insured_Last_Business_Name': Insured_Last_Business_Name,
                    'Insured_First_Name': Insured_First_Name,
                    'Insured_Polciy_No': Insured_Polciy_No,
                    'Insured_Claim_No': Insured_Claim_No,
                    'Attorney_Last_Name': attorney['Attorney_Last_Name'],
                    'Attorney_First_Name': attorney['Attorney_First_Name'],
                    'Attorney_Initial': attorney['Attorney_Initial'],
                    'Attorney_Street_Address': attorney['Attorney_Street_Address'],
                    'Attorney_City_State_Zip': attorney['Attorney_City_State_Zip'],
                    'Attorney_Email_Address': attorney['Attorney_Email_Address'],
                    'Violation_Insurer_Name': Violation_Insurer_Name,
                    'Violation_NAIC_Company_Code': Violation_NAIC_Company_Code,
                    'Individual_Responsible_for_viloation': Individual_Responsible_for_viloation,
                    'Violation_Type_of_Insurance': Violation_Type_of_Insurance,
                    'Violation_Reason_for_Notice': Vioaltion_Reason_for_Notice,
                    'Statutory_provisions_insurer_violated': Statutory_provisions_insurer_violated,
                    'Specific_policy_language': Specific_policy_language
                    # 'Facts_circumstances_giving_rise_to_the_violation': Facts_circumstances_giving_rise_to_the_violation
                }

                print(page)
                # if page == 260002: print(Facts_circumstances_giving_rise_to_the_violation)
            
                # for k,v in final_data.items(): 
                #     if type(v) == str: 
                #         if len(v) > 2000: print(k)

            except Exception as error:
                print(error)
                #print(f'could not retrive data for Filing_no {str(page)}')
                continue
    
            insert_script = '''
                    INSERT INTO civil_remedy_cases (
                                filing_no, 
                                Filing_date,
                                Complainant_Last_Business_Name,
                                Complainant_First_Name,
                                Complainant_Street_Address,
                                Complainant_City_State_ZIP,
                                Complainant_Email_Address,
                                Complainant_Type,
                                Insured_Last_Business_Name,
                                Insured_First_Name,
                                Insured_Polciy_No,
                                Insured_Claim_No,
                                Attorney_Last_Name,
                                Attorney_First_Name,
                                Attorney_Initial,
                                Attorney_Street_Address, 
                                Attorney_City_State_Zip, 
                                Attorney_Email_Address, 
                                Violation_Insurer_Name,
                                Violation_NAIC_Company_Code,
                                Individual_Responsible_for_viloation, 
                                Violation_Type_of_Insurance,
                                Violation_Reason_for_Notice,
                                Statutory_provisions_insurer_violated,
                                Specific_policy_language)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
            insert_value = tuple(final_data.values())
            try: 
                cur.execute(insert_script, insert_value)
            except Exception as error:
                print(error)
                conn.commit()
                break


        conn.commit()
    except Exception as error:
        print(error)
    finally:
        if cur is not None: cur.close()
        if conn is not None: conn.close()


if __name__ == '__main__':
    main()

