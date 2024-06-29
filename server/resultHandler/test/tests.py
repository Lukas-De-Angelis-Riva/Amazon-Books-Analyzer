import unittest
import shutil
import uuid
import os

from multiprocessing import Lock
from common.resultReceiver import ResultReceiver, BASE_DIRECTORY, TOTAL
from common.resultReceiver import QUERY1_ID, QUERY2_ID, QUERY3_ID, QUERY4_ID, QUERY5_ID
from utils.serializer.q1OutSerializer import Q1OutSerializer    # type: ignore
from utils.serializer.q2OutSerializer import Q2OutSerializer    # type: ignore
from utils.serializer.q3OutSerializer import Q3OutSerializer    # type: ignore
from utils.serializer.q5OutSerializer import Q5OutSerializer    # type: ignore
from dto.q3Result import Q3Result                               # type: ignore
from model.book import Book
from utils.middleware.testMiddleware2 import TestMiddleware2
from utils.model.message import Message, MessageType

from utils.model.virus import Disease, virus

TEST_RESULTS_DIRECTORY = '/test_results'

serializers = {
    QUERY1_ID: Q1OutSerializer(),
    QUERY2_ID: Q2OutSerializer(),
    QUERY3_ID: Q3OutSerializer(),
    QUERY4_ID: Q3OutSerializer(),
    QUERY5_ID: Q5OutSerializer(),
}


class TestUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.mkdir(TEST_RESULTS_DIRECTORY)
        if os.path.exists(BASE_DIRECTORY):
            shutil.rmtree(BASE_DIRECTORY)

    def setUp(self):
        virus.regenerate()
        return

    def append_eof(self, client_id, test_middleware, query_id, sent, eof_id=None):
        eof = Message(
            client_id=client_id,
            type=MessageType.EOF,
            data=b'',
            args={
                TOTAL: sent,
            }
        )
        if eof_id:
            eof.ID = eof_id
        test_middleware.add_message(eof.to_bytes(), query_id)

    def append_chunk(self, client_id, test_middleware, chunk, query_id, chunk_id=None):
        serializer = serializers[query_id]
        msg = Message(
            client_id=client_id,
            type=MessageType.DATA,
            data=serializer.to_bytes(chunk)
        )
        if chunk_id:
            msg.ID = chunk_id
        test_middleware.add_message(msg.to_bytes(), query_id)

    def make_q1_results(self):
        book1 = Book(
            title='Distributed Systems: Concepts and Design, 5th Edition',
            authors=['G. Coulouris', 'J. Dollimore', 't. Kindberg', 'G. Blair'],
            publisher='Addison Wesley',
            publishedDate='2012',
            categories=['Computers'],
        )

        book2 = Book(
            title='Distributed Systems, 3rd Edition',
            authors=['M. Van Steen', 'A. Tanenbaum'],
            publisher='Pearson Education',
            publishedDate='2017',
            categories=['Computers'],
        )

        book3 = Book(
            title='Distributed Systems for Systems Architects',
            authors=['P. Verissimo', 'L. Rodriguez'],
            publisher='Kluwer Academic Publishers',
            publishedDate='2001',
            categories=['Computers'],
        )

        book4 = Book(
            title='Designing Data-Intensive Applications',
            authors=['Martin Kleppmann'],
            publisher='O Reilly Media',
            publishedDate='2017',
            categories=['Computers'],
        )
        return book1, book2, book3, book4

    def make_q2_results(self):
        a1 = "George R. R. Martin"
        a2 = "J. R. R. Tolkien"
        a3 = "Howard Phillips Lovecraft"
        a4 = "Brandon Sanderson"
        return a1, a2, a3, a4

    def make_q3_results(self):
        r1 = Q3Result("Probabilidad y Estadística Elementales para Estudiantes de Ciencia", ['Ricardo Maronna'])
        r2 = Q3Result("Mathematical Statistics, Basic Ideas and Selected Topics", ['Bickel', 'Doksum'])
        r3 = Q3Result("Probability and Random Processes", ['Grimmet', 'Stirzaker'])
        r4 = Q3Result("Probability and Measure", ['Billingsley'])
        return r1, r2, r3, r4

    def make_q4_results(self):
        r1 = Q3Result("Probabilidad y Estadística Elementales para Estudiantes de Ciencia", ['Ricardo Maronna'])
        r2 = Q3Result("Mathematical Statistics, Basic Ideas and Selected Topics", ['Bickel', 'Doksum'])
        return r1, r2

    def make_q5_results(self):
        r1 = "A Game of Thrones"
        r2 = "The Silmarillion"
        r3 = "The Final Empire"
        r4 = "The Call of Cthulhu"
        return r1, r2, r3, r4

    def test_result_handler(self):
        client_id = uuid.UUID('00000000-0000-0000-0000-000000000000')

        test_middleware = TestMiddleware2()

        rq1_1, rq1_2, rq1_3, rq1_4 = self.make_q1_results()
        rq2_1, rq2_2, rq2_3, rq2_4 = self.make_q2_results()
        rq3_1, rq3_2, rq3_3, rq3_4 = self.make_q3_results()
        rq4_1, rq4_2 = self.make_q4_results()
        rq5_1, rq5_2, rq5_3, rq5_4 = self.make_q5_results()

        self.append_chunk(client_id, test_middleware, [rq1_1, rq1_2], QUERY1_ID)
        self.append_chunk(client_id, test_middleware, [rq1_3, rq1_4], QUERY1_ID)

        self.append_chunk(client_id, test_middleware, [rq2_1, rq2_2], QUERY2_ID)
        self.append_chunk(client_id, test_middleware, [rq2_3, rq2_4], QUERY2_ID)

        self.append_chunk(client_id, test_middleware, [rq3_1, rq3_2], QUERY3_ID)
        self.append_chunk(client_id, test_middleware, [rq3_3, rq3_4], QUERY3_ID)

        self.append_chunk(client_id, test_middleware, [rq4_1, rq4_2], QUERY4_ID)

        self.append_chunk(client_id, test_middleware, [rq5_1, rq5_2], QUERY5_ID)
        self.append_chunk(client_id, test_middleware, [rq5_3, rq5_4], QUERY5_ID)

        self.append_eof(client_id, test_middleware, QUERY1_ID, 4)
        self.append_eof(client_id, test_middleware, QUERY2_ID, 4)
        self.append_eof(client_id, test_middleware, QUERY3_ID, 4)
        self.append_eof(client_id, test_middleware, QUERY4_ID, 2)
        self.append_eof(client_id, test_middleware, QUERY5_ID, 4)

        resultHandler = ResultReceiver(TEST_RESULTS_DIRECTORY, Lock(), test_middleware)
        resultHandler.run()

        _len = len(resultHandler.clients[client_id].results)
        assert len(resultHandler.clients[client_id].results) == 19, f"Wrong size of results expected: {19}, but len(results): {_len}"

    def test_result_handler_premature_eof(self):
        client_id = uuid.UUID('10000000-0000-0000-0000-000000000000')

        test_middleware = TestMiddleware2()

        rq1_1, rq1_2, rq1_3, rq1_4 = self.make_q1_results()
        rq2_1, rq2_2, rq2_3, rq2_4 = self.make_q2_results()
        rq3_1, rq3_2, rq3_3, rq3_4 = self.make_q3_results()
        rq4_1, rq4_2 = self.make_q4_results()
        rq5_1, rq5_2, rq5_3, rq5_4 = self.make_q5_results()

        self.append_eof(client_id, test_middleware, QUERY1_ID, 4)
        self.append_eof(client_id, test_middleware, QUERY2_ID, 4)
        self.append_eof(client_id, test_middleware, QUERY3_ID, 4)
        self.append_eof(client_id, test_middleware, QUERY4_ID, 2)
        self.append_eof(client_id, test_middleware, QUERY5_ID, 4)

        self.append_chunk(client_id, test_middleware, [rq1_1, rq1_2], QUERY1_ID)
        self.append_chunk(client_id, test_middleware, [rq1_3, rq1_4], QUERY1_ID)

        self.append_chunk(client_id, test_middleware, [rq2_1, rq2_2], QUERY2_ID)
        self.append_chunk(client_id, test_middleware, [rq2_3, rq2_4], QUERY2_ID)

        self.append_chunk(client_id, test_middleware, [rq3_1, rq3_2], QUERY3_ID)
        self.append_chunk(client_id, test_middleware, [rq3_3, rq3_4], QUERY3_ID)

        self.append_chunk(client_id, test_middleware, [rq4_1, rq4_2], QUERY4_ID)

        self.append_chunk(client_id, test_middleware, [rq5_1, rq5_2], QUERY5_ID)
        self.append_chunk(client_id, test_middleware, [rq5_3, rq5_4], QUERY5_ID)

        resultHandler = ResultReceiver(TEST_RESULTS_DIRECTORY, Lock(), test_middleware)
        resultHandler.run()

        _len = len(resultHandler.clients[client_id].results)
        assert len(resultHandler.clients[client_id].results) == 19, f"Wrong size of results expected: {19}, but len(results): {_len}"

    def test_result_handler_sequential_multiclient(self):
        client_1 = uuid.UUID('20000000-0000-0000-0000-000000000000')
        client_2 = uuid.UUID('21000000-0000-0000-0000-000000000000')

        test_middleware = TestMiddleware2()

        rq1_1, rq1_2, rq1_3, rq1_4 = self.make_q1_results()
        rq2_1, rq2_2, rq2_3, rq2_4 = self.make_q2_results()
        rq3_1, rq3_2, rq3_3, rq3_4 = self.make_q3_results()
        rq4_1, rq4_2 = self.make_q4_results()
        rq5_1, rq5_2, rq5_3, rq5_4 = self.make_q5_results()

        self.append_chunk(client_1, test_middleware, [rq1_1, rq1_2], QUERY1_ID)
        self.append_chunk(client_1, test_middleware, [rq1_3, rq1_4], QUERY1_ID)
        self.append_chunk(client_1, test_middleware, [rq2_1, rq2_2], QUERY2_ID)
        self.append_chunk(client_1, test_middleware, [rq2_3, rq2_4], QUERY2_ID)
        self.append_chunk(client_1, test_middleware, [rq3_1, rq3_2], QUERY3_ID)
        self.append_chunk(client_1, test_middleware, [rq3_3, rq3_4], QUERY3_ID)
        self.append_chunk(client_1, test_middleware, [rq4_1, rq4_2], QUERY4_ID)
        self.append_chunk(client_1, test_middleware, [rq5_1, rq5_2], QUERY5_ID)
        self.append_chunk(client_1, test_middleware, [rq5_3, rq5_4], QUERY5_ID)
        self.append_eof(client_1, test_middleware, QUERY1_ID, 4)
        self.append_eof(client_1, test_middleware, QUERY2_ID, 4)
        self.append_eof(client_1, test_middleware, QUERY3_ID, 4)
        self.append_eof(client_1, test_middleware, QUERY4_ID, 2)
        self.append_eof(client_1, test_middleware, QUERY5_ID, 4)

        self.append_chunk(client_2, test_middleware, [rq1_1, rq1_2], QUERY1_ID)
        self.append_chunk(client_2, test_middleware, [rq2_1, rq2_2], QUERY2_ID)
        self.append_chunk(client_2, test_middleware, [rq3_1], QUERY3_ID)
        self.append_chunk(client_2, test_middleware, [rq4_1, rq4_2], QUERY4_ID)
        self.append_chunk(client_2, test_middleware, [rq5_1, rq5_2], QUERY5_ID)
        self.append_eof(client_2, test_middleware, QUERY1_ID, 2)
        self.append_eof(client_2, test_middleware, QUERY2_ID, 2)
        self.append_eof(client_2, test_middleware, QUERY3_ID, 2)
        self.append_eof(client_2, test_middleware, QUERY4_ID, 1)
        self.append_eof(client_2, test_middleware, QUERY5_ID, 2)

        resultHandler = ResultReceiver(TEST_RESULTS_DIRECTORY, Lock(), test_middleware)
        resultHandler.run()

        _len = len(resultHandler.clients[client_1].results)
        assert len(resultHandler.clients[client_1].results) == 19, f"Wrong size of results expected: {19}, but len(results): {_len}"
        _len = len(resultHandler.clients[client_2].results)
        assert len(resultHandler.clients[client_2].results) == 10, f"Wrong size of results expected: {10}, but len(results): {_len}"

    def test_result_handler_parallel_multiclient(self):
        client_1 = uuid.UUID('30000000-0000-0000-0000-000000000000')
        client_2 = uuid.UUID('31000000-0000-0000-0000-000000000000')

        test_middleware = TestMiddleware2()

        rq1_1, rq1_2, rq1_3, rq1_4 = self.make_q1_results()
        rq2_1, rq2_2, rq2_3, rq2_4 = self.make_q2_results()
        rq3_1, rq3_2, rq3_3, rq3_4 = self.make_q3_results()
        rq4_1, rq4_2 = self.make_q4_results()
        rq5_1, rq5_2, rq5_3, rq5_4 = self.make_q5_results()

        # -- -- -- -- CHAOS -- -- -- --
        self.append_chunk(client_2, test_middleware, [rq1_1, rq1_2], QUERY1_ID)
        self.append_eof(client_2, test_middleware, QUERY2_ID, 2)
        self.append_chunk(client_2, test_middleware, [rq5_1, rq5_2], QUERY5_ID)
        self.append_eof(client_1, test_middleware, QUERY1_ID, 4)
        self.append_eof(client_2, test_middleware, QUERY1_ID, 2)
        self.append_eof(client_1, test_middleware, QUERY3_ID, 4)
        self.append_chunk(client_2, test_middleware, [rq3_1], QUERY3_ID)
        self.append_chunk(client_2, test_middleware, [rq4_1, rq4_2], QUERY4_ID)
        self.append_chunk(client_1, test_middleware, [rq5_1, rq5_2], QUERY5_ID)
        self.append_eof(client_1, test_middleware, QUERY5_ID, 4)
        self.append_eof(client_1, test_middleware, QUERY4_ID, 2)
        self.append_chunk(client_1, test_middleware, [rq2_3, rq2_4], QUERY2_ID)
        self.append_eof(client_1, test_middleware, QUERY2_ID, 4)
        self.append_chunk(client_1, test_middleware, [rq2_1, rq2_2], QUERY2_ID)
        self.append_chunk(client_1, test_middleware, [rq5_3, rq5_4], QUERY5_ID)
        self.append_chunk(client_1, test_middleware, [rq4_1, rq4_2], QUERY4_ID)
        self.append_chunk(client_1, test_middleware, [rq3_3, rq3_4], QUERY3_ID)
        self.append_eof(client_2, test_middleware, QUERY5_ID, 2)
        self.append_chunk(client_2, test_middleware, [rq2_1, rq2_2], QUERY2_ID)
        self.append_eof(client_2, test_middleware, QUERY4_ID, 1)
        self.append_eof(client_2, test_middleware, QUERY3_ID, 2)
        self.append_chunk(client_1, test_middleware, [rq3_1, rq3_2], QUERY3_ID)
        self.append_chunk(client_1, test_middleware, [rq1_1, rq1_2], QUERY1_ID)
        self.append_chunk(client_1, test_middleware, [rq1_3, rq1_4], QUERY1_ID)

        resultHandler = ResultReceiver(TEST_RESULTS_DIRECTORY, Lock(), test_middleware)
        resultHandler.run()

        _len = len(resultHandler.clients[client_1].results)
        assert len(resultHandler.clients[client_1].results) == 19, f"Wrong size of results expected: {19}, but len(results): {_len}"
        _len = len(resultHandler.clients[client_2].results)
        assert len(resultHandler.clients[client_2].results) == 10, f"Wrong size of results expected: {10}, but len(results): {_len}"

    def test_result_handler_recovery(self):
        client_id = uuid.UUID('40000000-0000-0000-0000-000000000000')

        test_middleware = TestMiddleware2()
        a1 = "George R. R. Martin"
        a2 = "J. R. R. Tolkien"
        a3 = "Howard Phillips Lovecraft"
        a4 = "Brandon Sanderson"

        lines = [QUERY2_ID+', ' + a1 + '\n',
                 QUERY2_ID+', ' + a2 + '\n',
                 QUERY2_ID+', ' + a3 + '\n']

        with open(TEST_RESULTS_DIRECTORY+'/'+str(client_id)+'.csv', 'w', encoding='utf-8') as fp, \
             open(TEST_RESULTS_DIRECTORY+'/'+str(client_id)+'.csv.ptrs', 'wb') as ptrs_fp:
            ptrs_fp.write(int.to_bytes(fp.tell(), length=4, byteorder='big'))
            fp.writelines(lines[:2])
            ptrs_fp.write(int.to_bytes(fp.tell(), length=4, byteorder='big'))
            fp.writelines(lines[2:3])

        self.append_chunk(client_id, test_middleware, [a3, a4], QUERY2_ID)

        self.append_eof(client_id, test_middleware, QUERY1_ID, 0)
        self.append_eof(client_id, test_middleware, QUERY2_ID, 4)
        self.append_eof(client_id, test_middleware, QUERY3_ID, 0)
        self.append_eof(client_id, test_middleware, QUERY4_ID, 0)
        self.append_eof(client_id, test_middleware, QUERY5_ID, 0)

        resultHandler = ResultReceiver(TEST_RESULTS_DIRECTORY, Lock(), test_middleware)
        resultHandler.run()

        _len = len(resultHandler.clients[client_id].results)
        assert len(resultHandler.clients[client_id].results) == 5, f"Wrong size of results expected: {5}, but len(results): {_len}"

    def test_infected_result_handler(self):
        client_id = uuid.UUID('50000000-0000-0000-0000-000000000000')

        test_middleware = TestMiddleware2()

        rq1_1, rq1_2, rq1_3, rq1_4 = self.make_q1_results()
        rq2_1, rq2_2, rq2_3, rq2_4 = self.make_q2_results()
        rq3_1, rq3_2, rq3_3, rq3_4 = self.make_q3_results()
        rq4_1, rq4_2 = self.make_q4_results()
        rq5_1, rq5_2, rq5_3, rq5_4 = self.make_q5_results()

        self.append_eof(client_id, test_middleware, QUERY5_ID, 4)
        self.append_chunk(client_id, test_middleware, [rq1_3, rq1_4], QUERY1_ID)
        self.append_chunk(client_id, test_middleware, [rq3_3, rq3_4], QUERY3_ID)
        self.append_eof(client_id, test_middleware, QUERY1_ID, 4)
        self.append_chunk(client_id, test_middleware, [rq4_1, rq4_2], QUERY4_ID)
        self.append_chunk(client_id, test_middleware, [rq2_3, rq2_4], QUERY2_ID)
        self.append_eof(client_id, test_middleware, QUERY3_ID, 4)
        self.append_chunk(client_id, test_middleware, [rq5_1, rq5_2], QUERY5_ID)
        self.append_chunk(client_id, test_middleware, [rq5_3, rq5_4], QUERY5_ID)
        self.append_chunk(client_id, test_middleware, [rq1_1, rq1_2], QUERY1_ID)
        self.append_eof(client_id, test_middleware, QUERY2_ID, 4)
        self.append_chunk(client_id, test_middleware, [rq3_1, rq3_2], QUERY3_ID)
        self.append_eof(client_id, test_middleware, QUERY4_ID, 2)
        self.append_chunk(client_id, test_middleware, [rq2_1, rq2_2], QUERY2_ID)

        virus.mutate(0.15)
        while True:
            try:
                resultHandler = ResultReceiver(TEST_RESULTS_DIRECTORY, Lock(), test_middleware)
                resultHandler.run()
                break
            except Disease:
                continue

        _len = len(resultHandler.clients[client_id].results)
        assert len(resultHandler.clients[client_id].results) == 19, f"Wrong size of results expected: {19}, but len(results): {_len}"


if __name__ == '__main__':
    unittest.main()
