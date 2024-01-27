from requests_html import HTMLSession
import csv
import os
from datetime import datetime

folderpath ='/opt/airflow/dags/latestArticles/extract'

# main extractor////////
def get_articles(name:str,url:str, article_class:str, headline_selector:str, link_selector:str, image_selector:str, summary_selector:str,country:str,image_source='src'):
  
    session = HTMLSession()
    r = session.get(url)
    articles = r.html.find(article_class)

   
    if not os.path.exists(folderpath):
        os.makedirs(folderpath)

    with open(f'{folderpath}/{name}.csv', 'w', newline='',encoding='utf-8') as csvfile:
        articles_writer = csv.writer(csvfile)
        articles_writer.writerow(['headlines', 'links', 'image','summary','retrieval_date','website','maincountry'])
        ## loop through 
        for article in articles: 
            try:
                headline = article.find(headline_selector, first=True).text
                link = article.find(link_selector, first=True).attrs['href']
                image = article.find(image_selector, first=True).attrs[image_source]
                summary = article.find(summary_selector, first=True).text
                retrieval_date = datetime.today().strftime("%A, %d. %B %Y %I:%M%p")
                website = url
                maincountry = country

                articles_writer.writerow([headline, link, image, summary,retrieval_date,website,maincountry])
            except:
                pass
    return



   

            








def get_vanguard_articles(url="https://www.vanguardngr.com/"):
        try:
            articles = get_articles(
                name="varquard",
                url=url,
                article_class="article.entry-card-medium",
                headline_selector=".entry-title > a",
                link_selector=".entry-title > a",
                image_selector="img",
                summary_selector=".entry-title > a",
                image_source='src',
                country="nigeria"
            )

            articles = get_articles(
                name="latest_article",
                url=url,
                article_class="article.entry-list",
                headline_selector=".entry-title > a",
                link_selector=".entry-title > a",
                image_selector="img",
                summary_selector=".entry-excerpt p",
                image_source='src',
                country="nigeria"
            )

            articles = get_articles(
                name="feature_article",
                url=url,
                article_class="article.entry-featured",
                headline_selector=".entry-title > a",
                link_selector=".entry-title > a",
                image_selector="img",
                summary_selector=".entry-excerpt",
                image_source='src',
                country="nigeria"
            )
            return  articles
        except:
            pass
        return 




def get_thenation_news_article(name="thenationline",url="https://thenationonlineng.net/"):
    session = HTMLSession()
    r = session.get(url)
    article = r.html.find('a.nation_columnists_item')

    folderpath ='LatestArticles/'
    if not os.path.exists(folderpath):
        os.makedirs(folderpath)

    with open(f'{folderpath}/{name}.csv', 'w', newline='',encoding='utf-8') as csvfile:
        articles_writer = csv.writer(csvfile)
        articles_writer.writerow(['headline','link','image','summary','retrieval_date','website','maincountry'])
        ## loop through 

        for articles in article:
            try:
                #print(articles.html)
                headline = articles.find('h2.nation_columnists_name',first=True).text
                image = articles.find('img',first=True).attrs['src']
                link = articles.attrs['href']
                summary =  articles.find('h2.nation_columnists_name',first=True).text
                retrieval_date = datetime.today().strftime("%A, %d. %B %Y %I:%M%p")
                website = url
                maincountry = 'nigeria'
                articles_writer.writerow([headline,link,image,summary,retrieval_date,website,maincountry])
            except:
                pass
   
    return
        
   




def get_guardian_news_articles(name="guardian",url="https://guardian.ng/"):
    session = HTMLSession()
    r = session.get(url)
    article = r.html.find('div.design-article')

    folderpath ='LatestArticles/'
    if not os.path.exists(folderpath):
        os.makedirs(folderpath)
   
    with open(f'{folderpath}/{name}.csv', 'w', newline='',encoding='utf-8') as csvfile:
        articles_writer = csv.writer(csvfile)
        articles_writer.writerow(['headline', 'link', 'image','summary','"retrieval_date','website','maincountry'])

        for articles in article:
            try:
                headline = articles.find('span.title > a',first=True).text
                image = articles.find('img',first=True).attrs['src']
                link = articles.find("span.title > a",first=True).attrs['href']
                summary =  articles.find('div.excerpt',first=True).text 
                retrieval_date = datetime.today().strftime("%A, %d. %B %Y %I:%M%p")
                website = url
                maincountry = 'nigeria'
                articles_writer.writerow([headline,link,image,summary,retrieval_date,website,maincountry])
            except:
                pass

   



def get_this_day_articles(url="https://www.thisdaylive.com/"):
    try:
        articles = get_articles(
        name="this_day",
        url=url,
        article_class="article.typography",
        headline_selector="h2 > a",
        link_selector="div.article-image > a",
        image_selector="img",
        summary_selector="h2 > a",
        image_source='src',
        country="nigeria"
        )
        return  articles 
    except:
         pass   

    return  




def get_sun_newsonline_articles(url="https://www.sunnewsonline.com/"):
    try:
        articles = get_articles(
        name="sun_newsonline",
        url=url,
        article_class="article",
        headline_selector="h2.post-title",
        link_selector="h2.post-title > a",
        image_selector="img",
        summary_selector="h2.post-title > a",
        image_source='src',
        country="nigeria"
        )

        return  articles
    except:
        pass

    return 
   


def get_completes_sports_articles(url="https://www.completesports.com/"):
    try:
        articles = get_articles(name="completes_sports",
        url=url,
        article_class='div.simple-one',
        headline_selector='h3.item-title > a',
        link_selector='h3.item-title > a',
        image_selector='noscript > img',
        summary_selector='div.item-snippet',
        image_source='src',
        country="nigeria"
        )
        articles = get_articles(name="celebrity_news",
        url=url,
        article_class='div.carousel-item',
        headline_selector='h3.item-title > a',
        link_selector='h3.item-title > a',
        image_selector='noscript > img',
        summary_selector='h3.item-title > a',
        image_source='src',
        country="nigeria"
        )
    except:
        pass

    return  articles


def get_nigerianobservernews_articles(url="http://nigerianobservernews.com/"):
    try:
        articles = get_articles(name="nigerianobservernews",
        url=url,
        article_class='article',
        headline_selector='h2.post-title > a',
        link_selector='h2.post-title > a',
        image_selector='img',
        summary_selector='h2.post-title > a',
        image_source='data-src',
        country="nigeria"
        )
    except:
        pass

    return  articles


def get_premiumtimesng_articles(url="https://www.premiumtimesng.com/"):
    try:
        articles = get_articles(name="premiumtimesng",
        url=url,
        article_class='article',
        headline_selector='h3.jeg_post_title > a',
        link_selector='h3.jeg_post_title > a',
        image_selector='div.jeg_thumb > a > .thumbnail-container > img',
        summary_selector='h3.jeg_post_title > a',
        image_source='data-src',
        country="nigeria"
        )
    except:
        pass

    return  articles


def get_independentng_article(url="https://www.independent.ng/"):
    try:

        articles = get_articles(
            name='independentng_article',
            url=url,
            article_class='article',
            headline_selector='h2.title > a',
            link_selector='h2.title > a',
            image_selector='a[title]',
            summary_selector='h2.title > a',
            image_source='style',
            country="nigeria"
        )
    except:
        pass

    return  articles



def get_peoplesdailyng_article(url="http://peoplesdailyng.com/"):
    try:
        articles = get_articles(
            name="peoplesdailyng",
            url=url,
            article_class='div.wpb_wrapper',
            headline_selector='h5.entry-title > a',
            link_selector='h5.entry-title > a',
            image_selector='img',
            summary_selector='h5.entry-title > a',
            image_source='src',
            country="nigeria"
        )
    except:
        pass
    return  articles



def get_theabujatimes_article(url="https://www.theabujatimes.com/"):
    try:
        articles = get_articles('theabujatimes',
        url=url,
        article_class='div.td_module_flex',
        headline_selector='h3.entry-title > a',
        link_selector='h3.entry-title > a',
        image_selector='span.entry-thumb',
        summary_selector='div.td-excerpt',
        image_source='data-img-url',
        country="nigeria"
        ),
    except:
        pass

    return  articles


def get_dailypost_article(url="https://dailypost.ng/",name="dailypost"):
    session = HTMLSession()
    r = session.get(url)
    article = r.html.find('div.mvp-widget-feat1-bot-story')

    folderpath ='LatestArticles/'
    if not os.path.exists(folderpath):
        os.makedirs(folderpath)
   
    with open(f'{folderpath}/{name}.csv', 'w', newline='',encoding='utf-8') as csvfile:
        articles_writer = csv.writer(csvfile)
        articles_writer.writerow(['headline','link','image','summary','retrieval_date','website','maincountry'])

        for articles in article:
            try:
                #print(articles.html)
                headline = articles.find('h2',first=True).text
                image = articles.find('img',first=True).attrs['src']
                link = articles.find('img',first=True).attrs['srcset']
                summary =  articles.find('h2',first=True).text  
                retrieval_date = datetime.today().strftime("%A, %d. %B %Y %I:%M%p"),
                website = url,
                maincountry = "nigeria"
                
                articles_writer.writerow([headline, link, image, summary,retrieval_date,website,maincountry])
            except:
                pass
          
           

   



def get_saharareporters_articles(url="http://saharareporters.com/"):
    try:
        articles = get_articles(name="saharareporters",
        url=url,
        article_class='div.card-content',
        headline_selector='h2.title > a',
        link_selector='h2.title > a',
        image_selector='img',
        summary_selector='h2.title > a',
        image_source='srcset',
        country="nigeria")
    except:
        pass

    return  articles


def get_channelstv_articles(url="https://www.channelstv.com/"):
    try:
        articles = get_articles(name="channelstv",
        url=url,
        article_class='article',
        headline_selector='h3.post-title > a',
        link_selector='h3.post-title > a',
        image_selector='figure img',
        summary_selector='div.post-excerpt',
        image_source='src',
        country="nigeria")
    except:
        pass

    return  articles


def get_thecable_articles(url="https://www.thecable.ng/"):
    try:
        articles = get_articles(name="thecable_side",
        url=url,
        article_class='div.article-side-block',
        headline_selector='div.article-header > h2 > a',
        link_selector='div.article-header > h2 > a',
        image_selector='img',
        summary_selector='div.article-header > h2 > a',
        image_source='src',
        country="nigeria")

        articles = get_articles(name="thecable",
        url=url,
        article_class='.article-big-block',
        headline_selector='div.article-header > h2 > a',
        link_selector='div.article-header > h2 > a',
        image_selector='img',
        summary_selector='div.article-header > h2 > a',
        image_source='src',
        country="nigeria")
    except:
        pass

    return  articles


def get_dailynigerian_articles(url="https://dailynigerian.com/"):
    try:
        articles = get_articles(name="dailynigerian",
        url=url,
        article_class='div.td-module-container',
        headline_selector='h3.entry-title > a',
        link_selector='h3.entry-title > a',
        image_selector='div.td-image-container > .td-module-thumb > a > span',
        summary_selector='div.td-module-meta-info > .td-excerpt',
        image_source='data-bg',
        country="nigeria")
        return  articles
    except:
        pass

    return 


def get_graphic_articles(url="https://www.graphic.com.gh/"):
    try:
        articles = get_articles(name="graphic",
        url=url,
        article_class='div.mfp_newsy_item',
        headline_selector='h4.mfp_newsy_title > a',
        link_selector='h4.mfp_newsy_title > a',
        image_selector='img',
        summary_selector='h4.mfp_newsy_title > a',
        image_source='data-src',
        country="ghana")
        return  articles
    except:
        pass

    return 


def get_ghanaiantimes_articles(url="https://www.ghanaiantimes.com.gh/"):
    articles = get_articles(name="ghanaiantimes",
    url=url,
    article_class='li.post-item',
    headline_selector='h2.post-title > a',
    link_selector='h2.post-title > a',
    image_selector='img',
    summary_selector='p.post-excerpt',
    image_source='data-src',
    country="ghana")

    return  articles


def get_dailyguidenetwork_articles(url="https://dailyguidenetwork.com/"):
    articles = get_articles(name="dailyguidenetwork",
    url=url,
    article_class='article',
    headline_selector='h2.title > a',
    link_selector='h2.title > a',
    image_selector='div.image-container',
    summary_selector='h2.title > a',
    image_source='style',
    country="nigeria")

    return  articles



def get_yen_articles(url="https://yen.com.gh/"):
    articles = get_articles(name="yen",
    url=url,
    article_class='article',
    headline_selector='a',
    link_selector='a',
    image_selector='img',
    summary_selector='a',
    image_source='src',
    country="ghana")
    
    return  articles


def get_theheraldghana_articles(url="http://theheraldghana.com/"):
    articles = get_articles(name="theheraldghana_recent",
    url=url,
    article_class='div.p-wrap',
    headline_selector='h3.entry-title > a',
    link_selector='h3.entry-title > a',
    image_selector='img',
    summary_selector='h3.entry-title > a',
    image_source='src',
    country="ghana")

    return  articles


def get_modernghana_articles(url="https://www.modernghana.com/"):
    articles = get_articles(name="modernghana",
    url=url,
    article_class='div.thumbnail',
    headline_selector='div.caption1',
    link_selector='a',
    image_selector='img',
    summary_selector='div.caption1',
    image_source='src',
    country="ghana")

    return  articles


def get_nation_africa_articles(url="https://www.nation.co.ke/"):
    articles = get_articles(name="nation_africa",
    url=url,
    article_class='section > ol > li',
    headline_selector='h3',
    link_selector='a',
    image_selector='figure > img',
    summary_selector='h3',
    image_source='data-src',
    country="southafrica")

    return  articles


def get_tuko_articles(url="https://www.tuko.co.ke/"): 
    articles = get_articles(name="tuko",
    url=url,
    article_class='article',
    headline_selector='a',
    link_selector='a',
    image_selector='img',
    summary_selector='a',
    image_source='src',
    country="kenya")

    return  articles


def get_businessdailyafrica_articles(url="https://www.businessdailyafrica.com/"): 
    articles = get_articles(name="businessdailyafrica",
    url=url,
    article_class='article',
    headline_selector='h3',
    link_selector='a',
    image_selector='img',
    summary_selector='h3',
    image_source='data-cm-responsive-media',
    country="southafrica")

    return  articles


def get_kenyan_post_articles(url="https://kenyan-post.com/"):
    try:
        articles = get_articles(name="kenyan_post",
        url=url,
        article_class='div.td-block-span4',
        headline_selector='h3.entry-title > a',
        link_selector='h3.entry-title > a',
        image_selector='img',
        summary_selector='h3.entry-title > a',
        image_source='src',
        country="kenya")
        return articles
    except:
        pass
    return  



def get_the_star_articles(name="the_star",url="https://www.the-star.co.ke/"):
    session = HTMLSession()
    r = session.get(url)
    article = r.html.find('div.section-article')

    folderpath ='LatestArticles/'
    if not os.path.exists(folderpath):
        os.makedirs(folderpath)
   
    with open(f'{folderpath}/{name}.csv', 'w', newline='',encoding='utf-8') as csvfile:
        articles_writer = csv.writer(csvfile)
        articles_writer.writerow(['headline', 'link', 'image','summary','retrieval_date','website','maincountry'])
        for articles in article:
            try:
                #print(articles.html)
                headline = articles.find('h3.article-card-title',first=True).text
                link = articles.find('.article-body > a',first=True).attrs['href']
                summary = articles.find('p.article-synopsis',first=True).text
                image = articles.find('a.image',first=True).attrs['style']
                retrieval_date = datetime.today().strftime("%A, %d. %B %Y %I:%M%p")
                website = url,
                maincountry = 'kenya'
                
                articles_writer.writerow([headline,link,image,summary,retrieval_date,website,maincountry])

            except:
                    pass

   



def get_kenya_today_articles(url="https://www.kenya-today.com/"): 
    articles = get_articles(name="kenyan_today",
    url=url,
    article_class='article',
    headline_selector='h2.entry-title > a',
    link_selector='h2.entry-title > a',
    image_selector='img',
    summary_selector='div.entry-content',
    image_source='src',
    country="kenya")
    return  articles


def get_capitalfm_articles(url="https://www.capitalfm.co.ke/"): 
    articles = get_articles(name="capitalfm",
    url=url,
    article_class='article',
    headline_selector='h2.zox-s-title2',
    link_selector='div.zox-art-title > a',
    image_selector='img',
    summary_selector='p.zox-s-graph',
    image_source='src',
    country="kenya")

    articles = get_articles(name="capitalfm_latest",
    url=url,
    article_class='div.zox-art-grid',
    headline_selector='div.zox-art-title',
    link_selector='div.zox-art-title > a',
    image_selector='img',
    summary_selector='p.zox-s-graph',
    image_source='src',
    country="kenya")

    return  articles



def get_nairobiwire_articles(url="https://nairobiwire.com/"): 
    articles = get_articles(name="nairobiwire",
    url=url,
    article_class='div.block2-small-holder',
    headline_selector='div.cat-block-post-title > h3',
    link_selector='div.cat-block-post-title > h3 > a',
    image_selector='img',
    summary_selector='div.cat-block-content > p',
    image_source='data-lazy-src',
    country="kenya")

    return  articles


def get_businesstoday_articles(url="https://businesstoday.co.ke/"): 
    articles = get_articles(name="businesstoday_latest",
    url=url,
    article_class='div.tdb_module_loop',
    headline_selector='h3.entry-title',
    link_selector='h3.entry-title > a',
    image_selector='span.entry-thumb',
    summary_selector='div.td-excerpt',
    image_source='data-img-url',
    country="kenya")

    articles = get_articles(name="businesstoday_news",
    url=url,
    article_class='div.td_module_flex',
    headline_selector='h3.entry-title',
    link_selector='h3.entry-title > a',
    image_selector='span.entry-thumb',
    summary_selector='div.td-excerpt',
    image_source='data-img-url',
    country="globa")

    return  articles



def get_theeastafrican_articles(url="https://www.theeastafrican.co.ke/"): 
    articles = get_articles(name="theeastafrican",
    url=url,
    article_class='div.story-teaser',
    headline_selector='h3',
    link_selector='h3 > a',
    image_selector='figure  img',
    summary_selector='p',
    image_source='data-cm-responsive-media',
    country="kenya")

    return  articles



def get_dailysun_articles(url="https://www.dailysun.co.za/"): 
    articles = get_articles(name="dailysun",
    url=url,
    article_class='article',
    headline_selector='div.article-item__title',
    link_selector='a',
    image_selector='img',
    summary_selector='div.article-item__title',
    image_source='data-src',
    country="kenya")

    return  articles



def get_businesslive_articles(url="https://www.businesslive.co.za/bd/"): 
    articles = get_articles(name="businesslive",
    url=url,
    article_class='div.small-articles',
    headline_selector='a.article-title',
    link_selector='a.article-title',
    image_selector='span.image-loader-image',
    summary_selector='p.article-text',
    image_source='style',
    country="kenya")


    articles = get_articles(name="businesslive_opinion",
    url=url,
    article_class='div.col-sm-4',
    headline_selector='div.article-content h3 > a ',
    link_selector='div.article-content h3 > a',
    image_selector='span.image-loader-image',
    summary_selector='div.article-content p',
    image_source='style',
    country="kenya")

    return  articles



def get_dispatchlive_articles(url="https://www.dispatchlive.co.za/"): 
    articles = get_articles(name="dispatchlive",
    url=url,
    article_class='div.section-article',
    headline_selector='h3',
    link_selector='a',
    image_selector='span.image-loader-image',
    summary_selector='h3',
    image_source='style',
    country="world")

    return  articles



def get_cbn_co_za_articles(url="https://www.cbn.co.za/"): 
    articles = get_articles(name="cbn_co_za",
    url=url,
    article_class='div.td-block-span12',
    headline_selector='h3.entry-title > a',
    link_selector='h3.entry-title > a',
    image_selector='img',
    summary_selector='h3.entry-title > a',
    image_source='src',
    country="nigeria")

    return articles


def get_mg_co_za_articles(url="https://mg.co.za/"): 
    articles = get_articles(name="mg_co_za",
    url=url,
    article_class='article',
    headline_selector='h3 > a',
    link_selector='h3 > a',
    image_selector='img',
    summary_selector='h3 > a',
    image_source='src',
    country="nigeria")

    return  articles


def get_thesouthafrican_articles(url="https://www.thesouthafrican.com/"): 
    articles = get_articles(name="thesouthafrican",
    url=url,
    article_class='article',
    headline_selector='h3',
    link_selector='a',
    image_selector='img',
    summary_selector='h3',
    image_source='srcset',
    country="southAfrica")

    return  articles



def get_cameroonpostline_articles(url="https://cameroonpostline.com/"):
    try:
        articles = get_articles(name="cameroonpostline",
        url=url,
        article_class='article',
        headline_selector='h4.title > a',
        link_selector='h4.title > a',
        image_selector='div.mg-post-thumb',
        summary_selector='div.mg-content',
        image_source='style',
        country="cameroon"
        )
        return  articles
    except:
        pass

    return 


all_scrap_func = [
    get_vanguard_articles,
    get_thenation_news_article,
    get_guardian_news_articles,
    get_this_day_articles,
    get_sun_newsonline_articles,
    get_completes_sports_articles,
    get_nigerianobservernews_articles,
    get_premiumtimesng_articles,
    get_independentng_article,
    get_peoplesdailyng_article,
    get_theabujatimes_article,
    get_dailypost_article,
    get_saharareporters_articles,
    get_channelstv_articles,
    get_thecable_articles,
    get_dailynigerian_articles,
    get_graphic_articles,
    get_ghanaiantimes_articles,
    get_dailyguidenetwork_articles,
    get_yen_articles,
    get_theheraldghana_articles,
    get_modernghana_articles,
    get_nation_africa_articles,
    get_tuko_articles,
    get_businessdailyafrica_articles,
    get_kenyan_post_articles,
    get_the_star_articles,
    get_kenya_today_articles,
    get_capitalfm_articles,
    get_nairobiwire_articles,
    get_businesstoday_articles,
    get_theeastafrican_articles,
    get_dailysun_articles,
    get_businesslive_articles,
    get_dispatchlive_articles,
    get_cbn_co_za_articles,
    get_mg_co_za_articles,
    get_thesouthafrican_articles,
    get_cameroonpostline_articles,
]




def insert_scrap_data(all_scrap_func):
    print("scrapping article data from websites")
    for article_collection in all_scrap_func:
        article_collection()

