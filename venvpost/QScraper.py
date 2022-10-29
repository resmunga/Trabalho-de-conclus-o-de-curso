"""Created on Thu January 26 20:20:00 2022.

@author: lebarifouse & rafammat

Código criado para realizar raspagem de dados no Quora. Este código foi fruto
do projeto final de graduação:
    
"""

import scrapy
import scrapy.http

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from tqdm import tqdm


class SearchSpider(scrapy.Spider):
    """Classe de busca de Queries dentro do Quora."""

    name = 'quora_search_spider'

    custom_settings = {
        'DOWNLOAD_DELAY': 0.05,
        'CONCURRENT_REQUESTS': 10,
    }

    def __init__(self, queries: dict, requests_params: dict,
                 client: MongoClient, result_type="question"):
        """
        Classe derivada de scrapy.Spider criada para coleta de dados do Quora.

        Parameters
        ----------
        queries : dict
            Queries que serão utilizadas nas requisições, no formato:
                {chave: [valor1, valor2, ...]}.
        requests_params : dict
            Parâmetros da requisição.
        client : MongoClient
            Cliente do MongoDB.
        result_type : TYPE, optional
            Filtro do tipo de dado buscado. The default is "question".
            Deve ser um dos valores: ["all_types", "question", "answer",
                                      "post", "profile", "topic", "tribe"].

        Raises
        ------
        ValueError
            Erro caso o result_type seja definido fora dos padrões.
            Erro caso o requests_params seja definido fora dos padrões.
        PermissionError
            Erro caso não seja possível conectar ao MongoDB.

        Returns
        -------
        None.

        """
        super().__init__()

        self.queries = queries

        if result_type in ["all_types", "question", "answer", "post",
                           "profile", "topic", "tribe"]:
            self.result_type = result_type
        else:
            raise ValueError("Especifique o result_type corretamente.")

        # Atributos da request da página de busca
        try:
            self.url = requests_params['search-page']['url']
            self.headers = requests_params['search-page']['headers']
            self.payload = requests_params['search-page']['payload']
            self.cookies = requests_params['search-page']['cookies']
            self.headers['user-agent'] += str(requests_params['user-agent'])
        except Exception:
            raise ValueError("Verifique o arquivo de parâmetros.")

        # Atribuindo o banco de dados
        if not isinstance(client, MongoClient):
            raise ValueError("É necessário definir um cliente do MongoDB.")
        try:
            self._db = client["quora_database"]
            self._db["tmp"].drop()
        except Exception as e:
            raise PermissionError(e)

        for cat in queries:
            # inserindo category inéditas no banco - coleção category
            try:
                self._db['category'].insert_one({"_id": cat})
            except DuplicateKeyError:
                pass

            for q in queries[cat]:
                # inserindo queries inéditas no banco - coleção query
                try:
                    self._db['query'].insert_one(
                        {"_id": q})
                except DuplicateKeyError:
                    pass

                # inserindo relações category-query inéditas no banco
                # - coleção category_query
                try:
                    self._db['category_query'].insert_one(
                        {"_id": f"{cat}_{q}",
                         "query": q,
                         "category": cat}
                    )
                except DuplicateKeyError:
                    pass

    def start_requests(self):
        """Gerencia as requisições do Scrapy."""
        self.hasNextPage = dict()
        for category, queries in self.queries.items():
            for query in queries:
                # utilizado no loop de coleta de dados.
                self.hasNextPage[query] = True

                # variáveis de coleta de dados
                self._after = -1
                self.payload["variables"]["after"] = str(self._after)
                self.payload["variables"]["query"] = query
                self.payload["variables"]["resultType"] = self.result_type

                while True:
                    try:
                        yield scrapy.http.JsonRequest(
                            url=self.url,
                            headers=self.headers,
                            data=self.payload,
                            cookies=self.cookies,
                            callback=self.parse,
                            cb_kwargs={'query': query,
                                       'category': category}
                        )
                    except Exception as e:
                        print(e)
                    if not self.hasNextPage[query]:
                        break

    def parse(self, response, category, query):
        """
        Analisa o conteúdo de cada requisição.

        Parameters
        ----------
        response : requests.Response
            Resposta da requisição.
        category : str
            Categoria da Query buscada.
        query : str
            Query buscada.

        Returns
        -------
        None.

        """
        result = response.json()

        searchConnection = result["data"]["searchConnection"]

        if searchConnection is not None:
            edges = searchConnection["edges"]

            iteracao = (self._after + 1) // 10
            for item in tqdm(edges,
                             desc=f"Crawling {iteracao}: {category}: {query}"):
                tipo = item["node"]["searchResultType"]
                url = "https://www.quora.com"

                if tipo == "question":
                    qid = item["node"][tipo]["qid"]
                    url += item["node"][tipo]["url"]
                    question = item["node"][tipo]
                    question["_id"] = qid
                    try:
                        # inserindo relações category-query-question inéditas
                        # no banco - coleção category_query_question
                        self._db["category_query_qid"].insert_one(
                            {"_id": f"{category}_{query}_{qid}",
                             "category_query": f"{category}_{query}",
                             "qid": qid}
                        )
                    except DuplicateKeyError:
                        pass

                    # Identificando questões não respondidas
                    if url[:33] != "https://www.quora.com/unanswered/":
                        try:
                            # inserindo question respondida no banco para
                            # posterior coleta de respostas - coleção tmp
                            self._db["tmp"].insert_one({"category": category,
                                                        "query": query,
                                                        "_id": qid})
                        except DuplicateKeyError:
                            pass

                    try:
                        # inserindo questions inéditas no banco - coleção
                        # questions
                        self._db["questions"].insert_one(question)
                    except DuplicateKeyError:
                        pass
                    
                elif tipo == "topic":
                    tid = item["node"][tipo]["tid"]
                    url += item["node"][tipo]["url"]
                    topic = item["node"][tipo]
                    topic["_id"] = tid
                    try:
                        self._db["category_query_tid"].insert_one(
                            {"_id": f"{category}_{query}_{tid}",
                             "category_query": f"{category}_{query}",
                             "tid": tid}
                        )
                    except DuplicateKeyError:
                        pass

                    try:
                        # inserindo topics inéditas no banco
                        self._db["topics"].insert_one(topic)
                    except DuplicateKeyError:
                        pass

            hasNextPage = searchConnection["pageInfo"].get("hasNextPage",
                                                           False)
            if not hasNextPage:
                # indica que não há próxima página
                self.hasNextPage[query] = False

            # Atualizando atributo da próxima request
            self._after += 10
            self.payload["variables"]["after"] = str(self._after)


###############################################################################
class AnswerSpider(scrapy.Spider):
    """Classe de coleta de dados de respostas da página de uma pergunta."""

    name = 'quora_answer_spider'

    custom_settings = {
        'DOWNLOAD_DELAY': 0.05,
        'CONCURRENT_REQUESTS': 5,
    }

    def __init__(self, requests_params: dict, client: MongoClient):
        """
        Inicializa a instância de coleta de perguntas.

        Parameters
        ----------
        requests_params : dict
            Parâmetros de coleta de dados.
        client : MongoClient
            Cliente do MongoDB.

        Raises
        ------
        ValueError
            Erro caso o requests_params seja definido fora dos padrões.

        Returns
        -------
        None.

        """
        super().__init__()

        self._db = client["quora_database"]

        try:
            self.url = requests_params['question-page']['url']
            self.headers = requests_params['question-page']['headers']
            self.payload = requests_params['question-page']['payload']
            self.cookies = requests_params['question-page']['cookies']
            self.headers['user-agent'] += str(requests_params['user-agent'])
        except Exception:
            raise ValueError("Verifique o arquivo de parâmetros.")

    def start_requests(self):
        """
        Início da requisição do Scrapy.

        É utilizado um banco intermediário chamado tmp para armazenar as
        perguntas que foram coletadas. Esse banco é lido e assim sabe-se qual
        pergunta precisa de coleta de respostas.
        """
        # Esta definição do tmp deve vir aqui, não colocá-la no init!
        tmp = self._db["tmp"].find()
        tmp = [line for line in tmp]

        self.hasNextPage = dict()
        for line in tqdm(tmp, desc="Lendo bd.tmp:"):
            category = line.get("category", "General")
            query = line.get("query")
            qid = line.get("_id")
            self.hasNextPage[qid] = True
            self._after = -1
            self.payload["variables"]["after"] = str(self._after)
            self.payload["variables"]["qid"] = qid

            while True:
                yield scrapy.http.JsonRequest(
                    url=self.url,
                    headers=self.headers,
                    data=self.payload,
                    cookies=self.cookies,
                    callback=self.parse,
                    cb_kwargs={'qid': qid,
                               'query': query,
                               'category': category}
                )
                if not self.hasNextPage[qid]:
                    break

        try:
            self._db["tmp"].drop()
        except Exception as e:
            print(e)

    def parse(self, response, category, query, qid):
        """
        Tratamento de dados da requisição.

        Parameters
        ----------
        response : requests.Response
            Resposta da requisição.
        category : str
            Categoria da Query.
        query : str
            Query utilizada.
        qid : int
            Identificação da questão dentro do sistema do Quora.

        Returns
        -------
        None.

        """
        result = response.json()
        pagedListDataConnection = result["data"]["question"][
            "pagedListDataConnection"]
        edges = pagedListDataConnection["edges"]

        iteracao = (self._after + 1) // 12
        for item in tqdm(edges,
                         desc=f"Parsing {iteracao} Answers of {qid}"):
            # pulando o que não é resposta
            if "answer" not in item["node"]:
                continue
            # QuestionAnswerItem2 -> é uma resposta da pergunta em específico
            if item["node"]["__typename"] != "QuestionAnswerItem2":
                continue

            answer = item["node"]["answer"]
            aid = answer["aid"]
            answer['_id'] = aid

            try:
                # Inserindo relações question-answer inéditas no banco -
                # coleção question_answer
                self._db["question_answer"].insert_one(
                    {"_id": f"{qid}_{aid}",
                     "qid": qid,
                     "aid": aid}
                )
            except DuplicateKeyError:
                pass

            try:
                # inserindo answers inéditas no banco - coleção answers
                self._db["answers"].insert_one(answer)
            except DuplicateKeyError:
                pass

        hasNextPage = pagedListDataConnection["pageInfo"].get("hasNextPage",
                                                              False)
        if not hasNextPage:
            # Indica que não existe próxima página
            self.hasNextPage[qid] = False

        # Atualizando atributo da próxima request
        self._after += 12  # after1 = first*n + after0
        self.payload["variables"]["after"] = str(self._after)

###############################################################################
class TopicSpider(scrapy.Spider):
    """Classe de coleta de dados de respostas da página de uma pergunta."""

    name = 'quora_topic_spider'

    custom_settings = {
        'DOWNLOAD_DELAY': 0.05,
        'CONCURRENT_REQUESTS': 5,
    }

    def __init__(self, requests_params: dict, client: MongoClient):
        super().__init__()

        self._db = client["quora_database"]

        try:
            self.url = requests_params['question-page']['url']
            self.headers = requests_params['question-page']['headers']
            self.payload = requests_params['question-page']['payload']
            self.cookies = requests_params['question-page']['cookies']
            self.headers['user-agent'] += str(requests_params['user-agent'])
        except Exception:
            raise ValueError("Verifique o arquivo de parâmetros.")
    
    
    
class PostSpider(scrapy.Spider):
    """Classe de coleta de dados de respostas da página de uma pergunta."""

    name = 'quora_topic_spider'

    custom_settings = {
        'DOWNLOAD_DELAY': 0.05,
        'CONCURRENT_REQUESTS': 5,
    }

    def __init__(self, requests_params: dict, client: MongoClient):
        super().__init__()

        self._db = client["quora_database"]

        try:
            self.url = requests_params['question-page']['url']
            self.headers = requests_params['question-page']['headers']
            self.payload = requests_params['question-page']['payload']
            self.cookies = requests_params['question-page']['cookies']
            self.headers['user-agent'] += str(requests_params['user-agent'])
        except Exception:
            raise ValueError("Verifique o arquivo de parâmetros.")

    def start_requests(self):
        posts = self._db["posts"].find()
        posts = [line for line in posts]

        self.hasNextPage = dict()
        for line in tqdm(posts, desc="Lendo bd.tmp:"):
            category = line.get("category", "General")
            query = line.get("query")
            tid = line.get("_id")
            self.hasNextPage[tid] = True
            self._after = 0
            self.payload["variables"]["multifeedNumBundlesOnClient"] = str(self._after)
            self.payload["variables"]["pagedata"] = tid

            while True:
                print("Pegando POSTS")
                yield scrapy.http.JsonRequest(
                    url=self.url,
                    headers=self.headers,
                    data=self.payload,
                    cookies=self.cookies,
                    callback=self.parse,
                    cb_kwargs={'qid': tid,
                               'query': query,
                               'category': category}
                )
                if not self.hasNextPage[tid]:
                    break

        try:
            self._db["tmp"].drop()
        except Exception as e:
            print(e)

    def parse(self, response, category, query, qid):
        result = response.json()
        pagedListDataConnection = result["data"]["question"][
            "pagedListDataConnection"]
        edges = pagedListDataConnection["edges"]

        iteracao = (self._after + 1) // 12
        for item in tqdm(edges,
                         desc=f"Parsing {iteracao} Answers of {qid}"):
            # pulando o que não é resposta
            if "answer" not in item["node"]:
                continue
            # QuestionAnswerItem2 -> é uma resposta da pergunta em específico
            if item["node"]["__typename"] != "QuestionAnswerItem2":
                continue

            answer = item["node"]["answer"]
            aid = answer["aid"]
            answer['_id'] = aid

            try:
                # Inserindo relações question-answer inéditas no banco -
                # coleção question_answer
                self._db["quest ion_answer"].insert_one(
                    {"_id": f"{qid}_{aid}",
                     "qid": qid,
                     "aid": aid}
                )
            except DuplicateKeyError:
                pass

            try:
                # inserindo answers inéditas no banco - coleção answers
                self._db["answers"].insert_one(answer)
            except DuplicateKeyError:
                pass

        hasNextPage = pagedListDataConnection["pageInfo"].get("hasNextPage",
                                                              False)
        if not hasNextPage:
            # Indica que não existe próxima página
            self.hasNextPage[qid] = False

        # Atualizando atributo da próxima request
        self._after += 12  # after1 = first*n + after0
        self.payload["variables"]["after"] = str(self._after)

