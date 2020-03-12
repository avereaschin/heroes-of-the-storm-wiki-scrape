import requests
from bs4 import BeautifulSoup
import mysql.connector
import re

# connect to hots database which has been created using the mysql command line client using a simple
# "CREATE DATABASE hots" statement
conn = mysql.connector.connect(user='root', passwd='pswd', 
                               host='localhost',
                               database='hots')

c = conn.cursor()

# create heroes_basic table that will store all the basic hero information
c.execute("""CREATE TABLE heroes_basic (name VARCHAR(30), title VARCHAR(30), role VARCHAR(30), franchise VARCHAR(30),
                                        price_gold SMALLINT, price_gems SMALLINT, release_date DATE, hero_id SMALLINT 
                                        AUTO_INCREMENT, CONSTRAINT pk_heroes_basic PRIMARY KEY (hero_id))""")

# create hero_stats table that will store all basic hero stats
c.execute("""CREATE TABLE hero_stats (entity_name VARCHAR(30), attack_type VARCHAR(30), health SMALLINT, health_regen FLOAT,
                                      resource VARCHAR(30), unit_radius FLOAT, attack_speed FLOAT, attack_range FLOAT,
                                      attack_damage SMALLINT, hero_id SMALLINT, FOREIGN KEY (hero_id) REFERENCES heroes_basic(hero_id),
                                      entity_id SMALLINT AUTO_INCREMENT, CONSTRAINT pk_hero_stats PRIMARY KEY (entity_id))""")

def getLinks():
    """
    Scrapes internal links for each hero wiki
    """
    url = requests.get('https://heroesofthestorm.gamepedia.com/Heroes_of_the_Storm_Wiki')
    bs = BeautifulSoup(url.text, features='html.parser')

    # stores hero links
    links = set()

    # Find all internal hero links
    div = bs.find_all('div', {'class': 'link'})

    for i in div:
        # include distinct links only
        if i.find('a').attrs['href'] not in links:
            links.add(i.find('a').attrs['href'])

    return links

def getBasicInfo(link):
    """"
    Scrapes basic information from a hero's wiki page (hero name, role, difficulty, franchise, price etc.) and inserts the 
    data into the heroes_basic table of the hots database
    """

    url = requests.get('https://heroesofthestorm.gamepedia.com' + link)
    bs = BeautifulSoup(url.text, features='html.parser')

    # scrape table headers
    headers = bs.find('table', {'class': 'infobox2'}).find_all('th')
    # scape table cells 
    desc = bs.find('table', {'class': 'infobox2'}).find_all('td')

    # remove redundant headers from the headers list
    for i, j in enumerate(headers):
        if j.get_text() in ['Basic Info\n', 'Base Stats\n', 'Data Page\n']:
            headers.pop(i)

    *a, price, rd = [headers[0].get_text().strip()] + [j.get_text().strip() for j in desc][1:7]

    # some hero wiki pages don't include the difficulty of the hero so below code accounts for that
    if len(a) == 5:
        c.execute("""INSERT INTO heroes_basic (name, title, role, difficulty, franchise, price_gold, price_gems, release_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, STR_TO_DATE(%s, '%M %d, %Y'))""",
            (*a, *[int(i.replace(',', '')) for i in price.split(' / ')], rd))
    else:
        c.execute("""INSERT INTO heroes_basic (name, title, role, franchise, price_gold, price_gems, release_date)
            VALUES (%s, %s, %s, %s, %s, %s, STR_TO_DATE(%s, '%M %d, %Y'))""",
            (*a, *[int(i.replace(',', '')) for i in price.split(' / ')], rd))

def getStats(link):
    """
    Scrapes the basic stats (hp, resource, attack damage etc.) from a heroe's wiki page and inserts the data into the hero_stats
    table of the hots database
    """

    url = requests.get('https://heroesofthestorm.gamepedia.com' + link)
    bs = BeautifulSoup(url.text, features='html.parser')

    # some heroes are comprised of multiple entities, e.g. the Lost Vikings are made up of three characters who have
    # individual stats. These are presented on the website in separate tables so we need to scrape all stat tables 
    # from the wiki, hence the use of the bs.find_all function
    tables = bs.find_all('table', {'class': 'infobox2'})

    hero_title = bs.find('h1').get_text().replace('\n', '')

    # list containing all of the scraped data
    result = list()

    for i in tables:
        
        # grab all header and cell tags from each table
        table = i.find_all(['th', 'td'])
        for j in table:
            if j.get_text().strip() == 'Base Stats':
                result.append(i.find('th', {'class': 'name'}).get_text().strip())

                for k in table[table.index(j):]:
                    if k.name == 'td':
                        # regex check if string is a float number
                        if re.match(r'^[0-9]+\.[0-9]+$', k.get_text().strip()):
                            result.append(float(k.get_text().stri()))
                        # regex check if string is an int
                        elif re.match(r'^[0-9]+$', k.get_text().strip()):
                            result.append(int(k.get_text().strip()))
                        # check if cell is empty or has None value
                        elif k.get_text().strip() in ['', 'None']:
                            result.append(None)
                        else:
                            result.append(k.get_text().strip())

                c.execute("""INSERT INTO hero_stats (entity_name, attack_type, health, health_regen,
                                            resource, unit_radius, attack_speed, attack_range,
                                            attack_damage, hero_id, entity_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, (SELECT hero_id FROM heroes_basic WHERE name = %s), NULL)""",
                    (*result, hero_title))
                result = list()
                break

for link in links:
    getBasicInfo(link), getStats(link)

# commit changes made to the database and close the connection
conn.commit()
conn.close()
