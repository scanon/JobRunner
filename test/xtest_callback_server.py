# -*- coding: utf-8 -*-
import cherrypy
from cherrypy.test import helper
from utils.callback_server import Callback
from unittest.mock import create_autospec
from runner.MethodRunner import MethodRunner
import json

class CallbackServerTest(helper.CPWebCase):
    mr = create_autospec(MethodRunner)
    def setup_server():
        config = {}
        cb = Callback(config, CallbackServerTest.mr)
        conf = {
            '/': {
                'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
                'tools.sessions.on': True,
                'tools.response_headers.on': True,
                'tools.response_headers.headers': [('Content-Type', 'text/plain')],
            }
        }
        cherrypy.tree.mount(cb, '/', conf)
        cherrypy.config.update({'log.screen': False,
                        'log.access_file': '',
                        'log.error_file': ''})
    setup_server = staticmethod(setup_server)

    def xtest_message_should_be_returned_as_is(self):
        self.getPage("/")
        self.assertStatus('200 OK')
        # self.assertHeader('Content-Type', 'text/html;charset=utf-8')
        # self.assertBody('Hello world')

    def test_submit(self):
        body = json.dumps({
            'method': 'app._bogus_submit',
            'params': ['1234']
        })
        headers = [('Content-Type', 'application/json'),
                   ('Content-Length', str(len(body)))]
        out = self.getPage("/", method='POST', body=body, headers=headers)
        print(out)
        self.assertStatus('200 OK')

        body = json.dumps({
            'method': 'app._check_job',
            'params': ['1234']
        })
        CallbackServerTest.mr.is_finished.return_value = True
        CallbackServerTest.mr.get_output.return_value = {}
        
        headers = [('Content-Type', 'application/json'),
                   ('Content-Length', str(len(body)))]
        out = self.getPage("/", method='POST', body=body, headers=headers)
        print(out)
        self.assertStatus('200 OK')
        self.assertBody(b'{"result": [{"finished": true}]}')

    def test_prov(self):
        body = json.dumps({
            'method': 'app.get_provenance',
            'params': ['1234']
        })
        
        headers = [('Content-Type', 'application/json'),
                   ('Content-Length', str(len(body)))]
        out = self.getPage("/", method='POST', body=body, headers=headers)
        print(out)
        self.assertStatus('200 OK')
        # self.assertHeader('Content-Type', 'text/html;charset=utf-8')
        # self.assertBody('Hello world')
