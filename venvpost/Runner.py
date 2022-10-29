"""Created on Wed March 16 21:00:43 2022.

@author: lebarifouse & rafammat

Código criado para realizar raspagem de dados no Quora. Este código foi fruto
do projeto final de graduação:
    
"""

import json

from os.path import abspath as _abspath
from os.path import isfile as _isfile
from pymongo import MongoClient as _MongoClient
from QScraper import AnswerSpider as _AnswerSpider
from QScraper import SearchSpider as _SearchSpider
from QScraper import TopicSpider as _TopicSpider
from QScraper import PostSpider as _PostSpider
from scrapy.crawler import CrawlerRunner as _CrawlerRunner
from scrapy.settings import Settings as _Settings
from twisted.internet import defer as _defer
from twisted.internet import reactor as _reactor


class QScrapeRunner:
    """Executor das classes SearchSpider e AnswerSpider."""

    def __init__(self, user: str, email: str, requests_params_path: str):
        #print(requests_params_path)
        """
        Executor das classes SearchSpider e AnswerSpider.

        Prepara as requisições, inclusive montando o user-agent descritivo
        para o Quora.

        Parameters
        ----------
        user : str
            Nome do usuário que utilizará o Qscraper.
        email : str
            e-mail do usuário que utilizará o Qscraper.
        requests_params_path : str
            Caminho completo para a pasta onde está localizado o JSON com os
            parâmetros de requisição.

        Returns
        -------
        None.

        """
        self._read_requests_params(user, email, requests_params_path)

    def _read_requests_params(self, user: str, email: str,
                              requests_params_path: str):
        if _isfile(requests_params_path):
            with open(requests_params_path) as f:
                p = json.loads(f.read())
                p["user-agent"] = {"user": user, "e-mail": email}
                self._requests_params = p
        else:
            raise FileExistsError("Especifique o requests_params_path "
                                  "corretamente.")

    def _read_keywords(self, filepath: str) -> dict:
        with open(filepath) as f:
            d = json.loads(f.read().lower())
        for key in d:
            if len(d[key]) == 0:
                raise ValueError(f"A categoria {key} está vazia.")
            # transformando kw em listas, se necessario
            d[key] = [d[key]] if isinstance(d[key], str) else d[key]
        return d

    def run(self, client: _MongoClient, keywords_path: str,
            search_result_type="question"):
        r"""
        Executor da coleta de dados.

        O arquivo JSON com as palavras-chave, deve estar no seguinte formato:
            {categoria: ["paravra-chave1",
                         "palavra-chave2",
                         "\"expressão buscada\""]},
            onde:
                paravra-chave[n]: é uma palavra simples, sem espaços;
                expressão-chave:
                    Conjunto de palavras, separadas por espaço, iniciando e
                    finalizando por contrabarra + aspas, onde a busca
                    necessariamente deve trazer os termos buscados, na ordem
                    colocada.

        Parameters
        ----------
        client : MongoClient
            Cliente do MongoDB.
        keywords_path : str
            Caminho completo para a pasta onde está localizado o JSON com as
            queries.
        search_result_type : str, optional
            Filtro de busca no Quora. Por default o runner buscará somente as
            perguntas e na sequência, as respostas das perguntas. The default
            is "question".
            O valor deverá ser um dos seguintes:
                "all_types": todos os tipos de itens;
                "question": somente as perguntas;
                "answer": somente as respostas;
                "post": somente postagens;
                "profile": somente perfis;
                "topic": somente tópicos;
                "tribe": somente comunidades.

        """
        queries = self._read_keywords(keywords_path)

        settings = _Settings()

        runner = _CrawlerRunner(settings)

        @_defer.inlineCallbacks
        def crawl():
            search_types={
                "question": _AnswerSpider,
                "topic":_TopicSpider,
                "post" : _PostSpider
            }
            yield runner.crawl(_SearchSpider, queries,
                               self._requests_params, client=client,
                               result_type=search_result_type)
            yield runner.crawl(search_types[search_result_type], self._requests_params,
                               client=client)
            _reactor.stop()

        crawl()
        _reactor.run()


if __name__ == "__main__":
    from os import sep as _os_sep

    # loc = f'etc{_os_sep}X509-cert-90734881775499626.pem'
    # uri = ("mongodb+srv://quora-scrape.io7ki.mongodb.net/myFirstDatabase?"
    #        "authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true"
    #        "&w=majority")
    # client = _MongoClient(uri , tls=True, tlsCertificateKeyFile=loc)

    uri = ("mongodb://localhost:27017/")
    client = _MongoClient(uri)



    qsr_answers = QScrapeRunner( 
        "Pedro",
        "pedrohenriqueresmunga@gmail.com",
        f'etc/requests_params_answers.json'
    )
    qsr_answers.run(client, f"etc{_os_sep}keywords.json", search_result_type="question")

    qsr_posts = QScrapeRunner(
        "Pedro",
        "pedrohenriqueresmunga@gmail.com",
        f'etc{_os_sep}requests_params_posts.json'
    )
    qsr_posts.run(client, f"etc{_os_sep}keywords.json", search_result_type = "post")