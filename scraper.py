import pandas
import requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup


def get_urls(data: pandas.DataFrame) -> tuple:
    """
    Filters dataframe from file and returns a list of urls
    :param data: full dataframe
    :return: ad urls in list format
    """
    google_ads = data.loc[data["Platform"] == "Google Search"]
    ads = google_ads.get('Ad_URL')
    advertisers = google_ads.get('Advertiser_Name')
    return list(ads), list(advertisers)


def get_redirect_url(url, sesh):
    return sesh.get(url).url


def extract_ids(url):
    path_parts = urlparse(url).path.split("/")
    try:
        advertiser_id = path_parts[2]
        creative_id = path_parts[4]
        return advertiser_id, creative_id
    except IndexError:
        print(path_parts)
        record_error_url(url, "index error in extract_ids")


def get_adserver_url(advertiser_id, creative_id, sesh):

    data = {'f.req': f'{{"1":"{advertiser_id}","2":"{creative_id}"}}'}

    response = sesh.post(
        'https://adstransparency.google.com/anji/_/rpc/LookupService/GetCreativeById',
        data=data,
    )
    try:
        data = response.json()["1"]["5"][0]
    except KeyError:
        return "key error"
    if "3" in data.keys():
        adserver_url = data["3"]["2"].split(" ")[1].split("=")[1].replace("'", "")
        return adserver_url
    else:
        return None


def make_request(ads_dict):
    for advertiser in ads_dict:
        url = ads_dict[advertiser]
        with requests.Session() as sesh:
            new_url = get_redirect_url(url, sesh)
            try:
                advertiser_id, creative_id = extract_ids(new_url)
                adserver_url = get_adserver_url(advertiser_id, creative_id, sesh)
                if adserver_url == "key error":
                    record_error_url(new_url, "there was a key error in get_adserver_url")
                elif adserver_url is not None:
                    ad_domain = get_ad_domain(adserver_url, sesh)
                    print(f"extracted {ad_domain} from {url}")
                    write_to_file(ad_domain, advertiser)
                else:
                    record_error_url(new_url, "adserver_url was none")
            except TypeError:
                record_error_url(url, "redirect url was incomplete")


def get_ad_domain(url, sesh):
    res = sesh.get(url)
    soup = BeautifulSoup(res.content, "lxml")
    elm = soup.find("div", role="link")
    if elm:
        return elm["aria-label"][1:-1]
    else:
        return None


def make_dict(ads, advertisers) -> dict:
    ads_dict = {}
    for i in range(0, len(ads)):
        ads_dict[advertisers[i]] = ads[i]
    return ads_dict


def record_error_url(ad_url: str, reason: str):
    with open("errors.txt", mode='a') as outfile:
        outfile.write(f"{ad_url} did not work because {reason}\n")
        outfile.close()


def write_to_file(domain: str, advertiser: str):
    with open("domains.txt", mode='a') as outfile:
        outfile.write(f"{advertiser}: {domain} \n")
        outfile.close()


def main():
    data = pandas.read_csv("campaign_disclosures_ads.csv", dtype=str)
    ads_list, advertisers_list = get_urls(data)
    ads_dict = make_dict(ads_list, advertisers_list)
    make_request(ads_dict)


if __name__ == "__main__":
    main()
