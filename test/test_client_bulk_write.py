# Copyright 2024-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test the client bulk write API."""

from __future__ import annotations

import os
import sys

from pymongo.operations import InsertOne, ReplaceOne, UpdateOne

sys.path[0:0] = [""]

from unittest.mock import Mock, patch

import pymongo
from bson.codec_options import CodecOptions
from pymongo.encryption_options import _HAVE_PYMONGOCRYPT, AutoEncryptionOpts
from pymongo.errors import (
    ClientBulkWriteException,
    DocumentTooLarge,
    InvalidOperation,
    NetworkTimeout,
)
from pymongo.synchronous import client_bulk as client_bulk_module
from pymongo.synchronous.client_bulk import _ClientBulk
from pymongo.write_concern import WriteConcern
from test import (
    IntegrationTest,
    UnitTest,
    client_context,
    unittest,
)
from test.utils_shared import (
    OvertCommandListener,
)

_IS_SYNC = True


class TestClientBulkWrite(IntegrationTest):
    @client_context.require_version_min(8, 0, 0, -24)
    def test_returns_error_if_no_namespace_provided(self):
        models = [InsertOne(document={"a": "b"})]
        with self.assertRaises(InvalidOperation) as context:
            self.client.bulk_write(models=models)
        self.assertIn(
            "MongoClient.bulk_write requires a namespace to be provided for each write operation",
            context.exception._message,
        )

    @client_context.require_version_min(8, 0, 0, -24)
    def test_handles_non_pymongo_error(self):
        with patch.object(
            _ClientBulk, "write_command", return_value={"error": TypeError("mock type error")}
        ):
            models = [InsertOne(namespace="db.coll", document={"a": "b"})]
            with self.assertRaises(ClientBulkWriteException) as context:
                self.client.bulk_write(models=models)
            self.assertIsInstance(context.exception.error, TypeError)
            self.assertFalse(hasattr(context.exception.error, "details"))

    @client_context.require_version_min(8, 0, 0, -24)
    def test_formats_write_error_correctly(self):
        models = [
            InsertOne(namespace="db.coll", document={"_id": 1}),
            InsertOne(namespace="db.coll", document={"_id": 1}),
        ]

        with self.assertRaises(ClientBulkWriteException) as context:
            self.client.bulk_write(models=models)

        write_error = context.exception.write_errors[0]
        self.assertEqual(write_error["idx"], 1)
        self.assertEqual(write_error["op"], {"insert": 0, "document": {"_id": 1}})


# https://github.com/mongodb/specifications/tree/master/source/crud/tests
# Note: tests 1 and 2 are in test_read_write_concern_spec.py
class TestClientBulkWriteCRUD(IntegrationTest):
    def setUp(self):
        super().setUp()
        self.max_write_batch_size = client_context.max_write_batch_size
        self.max_bson_object_size = client_context.max_bson_size
        self.max_message_size_bytes = client_context.max_message_size_bytes

    @client_context.require_version_min(8, 0, 0, -24)
    def test_3_batch_splits_if_num_operations_too_large(self):
        listener = OvertCommandListener()
        client = self.rs_or_single_client(event_listeners=[listener])

        models = []
        for _ in range(self.max_write_batch_size + 1):
            models.append(InsertOne(namespace="db.coll", document={"a": "b"}))
        self.addCleanup(client.db["coll"].drop)

        result = client.bulk_write(models=models)
        self.assertEqual(result.inserted_count, self.max_write_batch_size + 1)

        bulk_write_events = []
        for event in listener.started_events:
            if event.command_name == "bulkWrite":
                bulk_write_events.append(event)
        self.assertEqual(len(bulk_write_events), 2)

        first_event, second_event = bulk_write_events
        self.assertEqual(len(first_event.command["ops"]), self.max_write_batch_size)
        self.assertEqual(len(second_event.command["ops"]), 1)
        self.assertEqual(first_event.operation_id, second_event.operation_id)

    @client_context.require_version_min(8, 0, 0, -24)
    def test_4_batch_splits_if_ops_payload_too_large(self):
        listener = OvertCommandListener()
        client = self.rs_or_single_client(event_listeners=[listener])

        models = []
        num_models = int(self.max_message_size_bytes / self.max_bson_object_size + 1)
        b_repeated = "b" * (self.max_bson_object_size - 500)
        for _ in range(num_models):
            models.append(
                InsertOne(
                    namespace="db.coll",
                    document={"a": b_repeated},
                )
            )
        self.addCleanup(client.db["coll"].drop)

        result = client.bulk_write(models=models)
        self.assertEqual(result.inserted_count, num_models)

        bulk_write_events = []
        for event in listener.started_events:
            if event.command_name == "bulkWrite":
                bulk_write_events.append(event)
        self.assertEqual(len(bulk_write_events), 2)

        first_event, second_event = bulk_write_events
        self.assertEqual(len(first_event.command["ops"]), num_models - 1)
        self.assertEqual(len(second_event.command["ops"]), 1)
        self.assertEqual(first_event.operation_id, second_event.operation_id)

    @client_context.require_version_min(8, 0, 0, -24)
    @client_context.require_failCommand_fail_point
    def test_5_collects_write_concern_errors_across_batches(self):
        listener = OvertCommandListener()
        client = self.rs_or_single_client(
            event_listeners=[listener],
            retryWrites=False,
        )

        fail_command = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 2},
            "data": {
                "failCommands": ["bulkWrite"],
                "writeConcernError": {"code": 91, "errmsg": "Replication is being shut down"},
            },
        }
        with self.fail_point(fail_command):
            models = []
            for _ in range(self.max_write_batch_size + 1):
                models.append(
                    InsertOne(
                        namespace="db.coll",
                        document={"a": "b"},
                    )
                )
            self.addCleanup(client.db["coll"].drop)

            with self.assertRaises(ClientBulkWriteException) as context:
                client.bulk_write(models=models)
            self.assertEqual(len(context.exception.write_concern_errors), 2)  # type: ignore[arg-type]
            self.assertIsNotNone(context.exception.partial_result)
            self.assertEqual(
                context.exception.partial_result.inserted_count, self.max_write_batch_size + 1
            )

        bulk_write_events = []
        for event in listener.started_events:
            if event.command_name == "bulkWrite":
                bulk_write_events.append(event)
        self.assertEqual(len(bulk_write_events), 2)

    @client_context.require_version_min(8, 0, 0, -24)
    def test_6_collects_write_errors_across_batches_unordered(self):
        listener = OvertCommandListener()
        client = self.rs_or_single_client(event_listeners=[listener])

        collection = client.db["coll"]
        self.addCleanup(collection.drop)
        collection.drop()
        collection.insert_one(document={"_id": 1})

        models = []
        for _ in range(self.max_write_batch_size + 1):
            models.append(
                InsertOne(
                    namespace="db.coll",
                    document={"_id": 1},
                )
            )

        with self.assertRaises(ClientBulkWriteException) as context:
            client.bulk_write(models=models, ordered=False)
        self.assertEqual(len(context.exception.write_errors), self.max_write_batch_size + 1)  # type: ignore[arg-type]

        bulk_write_events = []
        for event in listener.started_events:
            if event.command_name == "bulkWrite":
                bulk_write_events.append(event)
        self.assertEqual(len(bulk_write_events), 2)

    @client_context.require_version_min(8, 0, 0, -24)
    def test_6_collects_write_errors_across_batches_ordered(self):
        listener = OvertCommandListener()
        client = self.rs_or_single_client(event_listeners=[listener])

        collection = client.db["coll"]
        self.addCleanup(collection.drop)
        collection.drop()
        collection.insert_one(document={"_id": 1})

        models = []
        for _ in range(self.max_write_batch_size + 1):
            models.append(
                InsertOne(
                    namespace="db.coll",
                    document={"_id": 1},
                )
            )

        with self.assertRaises(ClientBulkWriteException) as context:
            client.bulk_write(models=models, ordered=True)
        self.assertEqual(len(context.exception.write_errors), 1)  # type: ignore[arg-type]

        bulk_write_events = []
        for event in listener.started_events:
            if event.command_name == "bulkWrite":
                bulk_write_events.append(event)
        self.assertEqual(len(bulk_write_events), 1)

    @client_context.require_version_min(8, 0, 0, -24)
    def test_7_handles_cursor_requiring_getMore(self):
        listener = OvertCommandListener()
        client = self.rs_or_single_client(event_listeners=[listener])

        collection = client.db["coll"]
        self.addCleanup(collection.drop)
        collection.drop()

        models = []
        a_repeated = "a" * (self.max_bson_object_size // 2)
        b_repeated = "b" * (self.max_bson_object_size // 2)
        models.append(
            UpdateOne(
                namespace="db.coll",
                filter={"_id": a_repeated},
                update={"$set": {"x": 1}},
                upsert=True,
            )
        )
        models.append(
            UpdateOne(
                namespace="db.coll",
                filter={"_id": b_repeated},
                update={"$set": {"x": 1}},
                upsert=True,
            )
        )

        result = client.bulk_write(models=models, verbose_results=True)
        self.assertEqual(result.upserted_count, 2)
        self.assertEqual(len(result.update_results), 2)

        get_more_event = False
        for event in listener.started_events:
            if event.command_name == "getMore":
                get_more_event = True
        self.assertTrue(get_more_event)

    @client_context.require_version_min(8, 0, 0, -24)
    @client_context.require_no_standalone
    def test_8_handles_cursor_requiring_getMore_within_transaction(self):
        listener = OvertCommandListener()
        client = self.rs_or_single_client(event_listeners=[listener])

        collection = client.db["coll"]
        self.addCleanup(collection.drop)
        collection.drop()

        with client.start_session() as session:
            session.start_transaction()
            models = []
            a_repeated = "a" * (self.max_bson_object_size // 2)
            b_repeated = "b" * (self.max_bson_object_size // 2)
            models.append(
                UpdateOne(
                    namespace="db.coll",
                    filter={"_id": a_repeated},
                    update={"$set": {"x": 1}},
                    upsert=True,
                )
            )
            models.append(
                UpdateOne(
                    namespace="db.coll",
                    filter={"_id": b_repeated},
                    update={"$set": {"x": 1}},
                    upsert=True,
                )
            )
            result = client.bulk_write(models=models, session=session, verbose_results=True)

        self.assertEqual(result.upserted_count, 2)
        self.assertEqual(len(result.update_results), 2)

        get_more_event = False
        for event in listener.started_events:
            if event.command_name == "getMore":
                get_more_event = True
        self.assertTrue(get_more_event)

    @client_context.require_version_min(8, 0, 0, -24)
    @client_context.require_failCommand_fail_point
    def test_9_handles_getMore_error(self):
        listener = OvertCommandListener()
        client = self.rs_or_single_client(event_listeners=[listener])

        collection = client.db["coll"]
        self.addCleanup(collection.drop)
        collection.drop()

        fail_command = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 1},
            "data": {"failCommands": ["getMore"], "errorCode": 8},
        }
        with self.fail_point(fail_command):
            models = []
            a_repeated = "a" * (self.max_bson_object_size // 2)
            b_repeated = "b" * (self.max_bson_object_size // 2)
            models.append(
                UpdateOne(
                    namespace="db.coll",
                    filter={"_id": a_repeated},
                    update={"$set": {"x": 1}},
                    upsert=True,
                )
            )
            models.append(
                UpdateOne(
                    namespace="db.coll",
                    filter={"_id": b_repeated},
                    update={"$set": {"x": 1}},
                    upsert=True,
                )
            )

            with self.assertRaises(ClientBulkWriteException) as context:
                client.bulk_write(models=models, verbose_results=True)
            self.assertIsNotNone(context.exception.error)
            self.assertEqual(context.exception.error["code"], 8)
            self.assertIsNotNone(context.exception.partial_result)
            self.assertEqual(context.exception.partial_result.upserted_count, 2)
            self.assertEqual(len(context.exception.partial_result.update_results), 1)

        get_more_event = False
        kill_cursors_event = False
        for event in listener.started_events:
            if event.command_name == "getMore":
                get_more_event = True
            if event.command_name == "killCursors":
                kill_cursors_event = True
        self.assertTrue(get_more_event)
        self.assertTrue(kill_cursors_event)

    @client_context.require_version_min(8, 0, 0, -24)
    def test_10_returns_error_if_unacknowledged_too_large_insert(self):
        listener = OvertCommandListener()
        client = self.rs_or_single_client(event_listeners=[listener])

        b_repeated = "b" * self.max_bson_object_size

        # Insert document.
        models_insert = [InsertOne(namespace="db.coll", document={"a": b_repeated})]
        with self.assertRaises(DocumentTooLarge):
            client.bulk_write(models=models_insert, ordered=False, write_concern=WriteConcern(w=0))

        # Replace document.
        models_replace = [ReplaceOne(namespace="db.coll", filter={}, replacement={"a": b_repeated})]
        with self.assertRaises(DocumentTooLarge):
            client.bulk_write(models=models_replace, ordered=False, write_concern=WriteConcern(w=0))

    def _setup_namespace_test_models(self):
        # See prose test specification below for details on these calculations.
        # https://github.com/mongodb/specifications/tree/master/source/crud/tests#details-on-size-calculations
        _EXISTING_BULK_WRITE_BYTES = 1122
        _OPERATION_DOC_BYTES = 57
        _NAMESPACE_DOC_BYTES = 217

        # When compression is enabled, max_message_size is
        # smaller to account for compression message header.
        if client_context.client_options.get("compressors"):
            max_message_size_bytes = self.max_message_size_bytes - 16
        else:
            max_message_size_bytes = self.max_message_size_bytes

        ops_bytes = max_message_size_bytes - _EXISTING_BULK_WRITE_BYTES
        num_models = ops_bytes // self.max_bson_object_size
        remainder_bytes = ops_bytes % self.max_bson_object_size

        models = []
        b_repeated = "b" * (self.max_bson_object_size - _OPERATION_DOC_BYTES)
        for _ in range(num_models):
            models.append(
                InsertOne(
                    namespace="db.coll",
                    document={"a": b_repeated},
                )
            )
        if remainder_bytes >= _NAMESPACE_DOC_BYTES:
            num_models += 1
            b_repeated = "b" * (remainder_bytes - _OPERATION_DOC_BYTES)
            models.append(
                InsertOne(
                    namespace="db.coll",
                    document={"a": b_repeated},
                )
            )
        return num_models, models

    @client_context.require_version_min(8, 0, 0, -24)
    def test_11_no_batch_splits_if_new_namespace_is_not_too_large(self):
        listener = OvertCommandListener()
        client = self.rs_or_single_client(event_listeners=[listener])

        num_models, models = self._setup_namespace_test_models()
        models.append(
            InsertOne(
                namespace="db.coll",
                document={"a": "b"},
            )
        )
        self.addCleanup(client.db["coll"].drop)

        # No batch splitting required.
        result = client.bulk_write(models=models)
        self.assertEqual(result.inserted_count, num_models + 1)

        bulk_write_events = []
        for event in listener.started_events:
            if event.command_name == "bulkWrite":
                bulk_write_events.append(event)

        self.assertEqual(len(bulk_write_events), 1)
        event = bulk_write_events[0]

        self.assertEqual(len(event.command["ops"]), num_models + 1)
        self.assertEqual(len(event.command["nsInfo"]), 1)
        self.assertEqual(event.command["nsInfo"][0]["ns"], "db.coll")

    @client_context.require_version_min(8, 0, 0, -24)
    def test_11_batch_splits_if_new_namespace_is_too_large(self):
        listener = OvertCommandListener()
        client = self.rs_or_single_client(event_listeners=[listener])

        num_models, models = self._setup_namespace_test_models()
        c_repeated = "c" * 200
        namespace = f"db.{c_repeated}"
        models.append(
            InsertOne(
                namespace=namespace,
                document={"a": "b"},
            )
        )
        self.addCleanup(client.db["coll"].drop)
        self.addCleanup(client.db[c_repeated].drop)

        # Batch splitting required.
        result = client.bulk_write(models=models)
        self.assertEqual(result.inserted_count, num_models + 1)

        bulk_write_events = []
        for event in listener.started_events:
            if event.command_name == "bulkWrite":
                bulk_write_events.append(event)

        self.assertEqual(len(bulk_write_events), 2)
        first_event, second_event = bulk_write_events

        self.assertEqual(len(first_event.command["ops"]), num_models)
        self.assertEqual(len(first_event.command["nsInfo"]), 1)
        self.assertEqual(first_event.command["nsInfo"][0]["ns"], "db.coll")

        self.assertEqual(len(second_event.command["ops"]), 1)
        self.assertEqual(len(second_event.command["nsInfo"]), 1)
        self.assertEqual(second_event.command["nsInfo"][0]["ns"], namespace)

    @client_context.require_version_min(8, 0, 0, -24)
    def test_12_returns_error_if_no_writes_can_be_added_to_ops(self):
        client = self.rs_or_single_client()

        # Document too large.
        b_repeated = "b" * self.max_message_size_bytes
        models = [InsertOne(namespace="db.coll", document={"a": b_repeated})]
        with self.assertRaises(DocumentTooLarge) as context:
            client.bulk_write(models=models)
            self.assertIsNone(context.exception.partial_result)

        # Namespace too large.
        c_repeated = "c" * self.max_message_size_bytes
        namespace = f"db.{c_repeated}"
        models = [InsertOne(namespace=namespace, document={"a": "b"})]
        with self.assertRaises(DocumentTooLarge) as context:
            client.bulk_write(models=models)
            self.assertIsNone(context.exception.partial_result)

    @client_context.require_version_min(8, 0, 0, -24)
    @unittest.skipUnless(_HAVE_PYMONGOCRYPT, "pymongocrypt is not installed")
    def test_13_returns_error_if_auto_encryption_configured(self):
        opts = AutoEncryptionOpts(
            key_vault_namespace="db.coll",
            kms_providers={"aws": {"accessKeyId": "foo", "secretAccessKey": "bar"}},
        )
        client = self.rs_or_single_client(auto_encryption_opts=opts)

        models = [InsertOne(namespace="db.coll", document={"a": "b"})]
        with self.assertRaises(InvalidOperation) as context:
            client.bulk_write(models=models)
            self.assertIsNone(context.exception.partial_result)
        self.assertIn(
            "bulk_write does not currently support automatic encryption", context.exception._message
        )

    @client_context.require_version_min(8, 0, 0, -24)
    def test_upserted_result(self):
        client = self.rs_or_single_client()

        collection = client.db["coll"]
        self.addCleanup(collection.drop)
        collection.drop()

        models = []
        models.append(
            UpdateOne(
                namespace="db.coll",
                filter={"_id": "a"},
                update={"$set": {"x": 1}},
                upsert=True,
            )
        )
        models.append(
            UpdateOne(
                namespace="db.coll",
                filter={"_id": None},
                update={"$set": {"x": 1}},
                upsert=True,
            )
        )
        models.append(
            UpdateOne(
                namespace="db.coll",
                filter={"_id": None},
                update={"$set": {"x": 1}},
            )
        )
        result = client.bulk_write(models=models, verbose_results=True)

        self.assertEqual(result.upserted_count, 2)
        self.assertEqual(result.update_results[0].did_upsert, True)
        self.assertEqual(result.update_results[1].did_upsert, True)
        self.assertEqual(result.update_results[2].did_upsert, False)

    # Note: test 14 is optional and intentionally not implemented because we provide multiple APIs to specify explain.

    @client_context.require_version_min(8, 0, 0, -24)
    def test_15_unacknowledged_write_across_batches(self):
        listener = OvertCommandListener()
        client = self.rs_or_single_client(event_listeners=[listener])

        collection = client.db["coll"]
        self.addCleanup(collection.drop)
        collection.drop()
        client.db.command({"create": "db.coll"})

        b_repeated = "b" * (self.max_bson_object_size - 500)
        models = [
            InsertOne(namespace="db.coll", document={"a": b_repeated})
            for _ in range(int(self.max_message_size_bytes / self.max_bson_object_size) + 1)
        ]

        listener.reset()

        res = client.bulk_write(models, ordered=False, write_concern=WriteConcern(w=0))
        self.assertEqual(False, res.acknowledged)

        events = listener.started_events
        self.assertEqual(2, len(events))
        self.assertEqual(
            int(self.max_message_size_bytes / self.max_bson_object_size),
            len(events[0].command["ops"]),
        )
        self.assertEqual(1, len(events[1].command["ops"]))
        self.assertEqual(events[0].operation_id, events[1].operation_id)
        self.assertEqual({"w": 0}, events[0].command["writeConcern"])
        self.assertEqual({"w": 0}, events[1].command["writeConcern"])

        self.assertEqual(
            int(self.max_message_size_bytes / self.max_bson_object_size) + 1,
            collection.count_documents({}),
        )


# https://github.com/mongodb/specifications/blob/master/source/client-side-operations-timeout/tests/README.md#11-multi-batch-bulkwrites
class TestClientBulkWriteCSOT(IntegrationTest):
    def setUp(self):
        super().setUp()
        self.max_write_batch_size = client_context.max_write_batch_size
        self.max_bson_object_size = client_context.max_bson_size
        self.max_message_size_bytes = client_context.max_message_size_bytes

    @client_context.require_version_min(8, 0, 0, -24)
    @client_context.require_failCommand_fail_point
    @client_context.require_standalone
    def test_timeout_in_multi_batch_bulk_write(self):
        if sys.platform != "linux" and "CI" in os.environ:
            self.skipTest("PYTHON-3522 CSOT test runs too slow on Windows and MacOS")
        _OVERHEAD = 500

        internal_client = self.rs_or_single_client(timeoutMS=None)

        collection = internal_client.db["coll"]
        self.addCleanup(collection.drop)
        collection.drop()

        fail_command = {
            "configureFailPoint": "failCommand",
            "mode": {"times": 2},
            "data": {"failCommands": ["bulkWrite"], "blockConnection": True, "blockTimeMS": 1010},
        }
        with self.fail_point(fail_command):
            models = []
            num_models = int(self.max_message_size_bytes / self.max_bson_object_size + 1)
            b_repeated = "b" * (self.max_bson_object_size - _OVERHEAD)
            for _ in range(num_models):
                models.append(
                    InsertOne(
                        namespace="db.coll",
                        document={"a": b_repeated},
                    )
                )

            listener = OvertCommandListener()
            client = self.rs_or_single_client(
                event_listeners=[listener],
                readConcernLevel="majority",
                readPreference="primary",
                timeoutMS=2000,
                w="majority",
            )
            # Initialize the client with a larger timeout to help make test less flakey
            with pymongo.timeout(10):
                client.admin.command("ping")
            with self.assertRaises(ClientBulkWriteException) as context:
                client.bulk_write(models=models)
            self.assertIsInstance(context.exception.error, NetworkTimeout)

        bulk_write_events = []
        for event in listener.started_events:
            if event.command_name == "bulkWrite":
                bulk_write_events.append(event)
        self.assertEqual(len(bulk_write_events), 2)


class _FakeBulkWriteCursor:
    """A stand-in for the bulkWrite response cursor, yielding pre-baked documents."""

    def __init__(self, docs):
        self._docs = docs
        self.alive = True
        self.closed = False

    def _maybe_pin_connection(self, conn):
        pass

    def close(self):
        self.alive = False
        self.closed = True

    def __iter__(self):
        return self._iterate()

    def _iterate(self):
        for doc in self._docs:
            yield doc


class TestClientBulkWriteResponseValidation(UnitTest):
    """Test validation of idx values in client-level bulk write responses.

    These tests use locally fabricated server responses and do not
    require a running MongoDB server.
    """

    def _make_bulk(
        self, namespaces, ordered=True, verbose_results=False, max_write_batch_size=100000
    ):
        client = Mock()
        client.codec_options = CodecOptions()
        client.options.retry_writes = False
        conn = Mock()
        conn.compression_context = None
        conn.max_wire_version = 25
        conn.max_bson_size = 16777216
        conn.max_message_size = 48000000
        conn.max_write_batch_size = max_write_batch_size
        conn.address = ("localhost", 27017)

        def _retryable_write(retryable, func, session, operation, **kwargs):
            func(session, conn, False)

        client._retryable_write = _retryable_write

        bulk = _ClientBulk(
            client,
            WriteConcern(),
            ordered=ordered,
            verbose_results=verbose_results,
        )
        for idx, namespace in enumerate(namespaces):
            bulk.add_insert(namespace, {"_id": idx})
        return bulk

    def _run_bulk(self, bulk, replies):
        """Execute the bulk operation against fabricated server replies."""
        sent_batches = []
        self._sent_batches = sent_batches

        def fake_write_command(self_bulk, bwc, cmd, request_id, msg, op_docs, ns_docs, client):
            sent_batches.append(list(op_docs))
            return replies[len(sent_batches) - 1]

        def fake_cursor(coll, cursor_info, address, **kwargs):
            return _FakeBulkWriteCursor(cursor_info["firstBatch"])

        with (
            patch.object(_ClientBulk, "write_command", fake_write_command),
            patch.object(client_bulk_module, "CommandCursor", fake_cursor),
            patch.object(client_bulk_module, "Database", Mock()),
            patch.object(client_bulk_module, "Collection", Mock()),
        ):
            return bulk.execute(None, "bulkWrite")

    @staticmethod
    def _reply(docs, n_inserted):
        return {
            "ok": 1,
            "nInserted": n_inserted,
            "nErrors": sum(1 for doc in docs if not doc["ok"]),
            "cursor": {"id": 0, "ns": "admin.$cmd.bulkWrite", "firstBatch": docs},
        }

    def test_valid_multi_batch_idx_mapping(self):
        bulk = self._make_bulk(
            ["db.coll1", "db.coll2", "db.coll1"],
            verbose_results=True,
            max_write_batch_size=2,
        )
        replies = [
            self._reply([{"idx": 0, "ok": 1}, {"idx": 1, "ok": 1}], n_inserted=2),
            self._reply([{"idx": 0, "ok": 1}], n_inserted=1),
        ]
        result = self._run_bulk(bulk, replies)
        self.assertEqual(len(self._sent_batches), 2)
        self.assertEqual(len(self._sent_batches[0]), 2)
        self.assertEqual(len(self._sent_batches[1]), 1)
        self.assertEqual(result.inserted_count, 3)
        self.assertEqual(set(result.insert_results), {0, 1, 2})
        self.assertEqual(result.insert_results[0].inserted_id, 0)
        self.assertEqual(result.insert_results[1].inserted_id, 1)
        self.assertEqual(result.insert_results[2].inserted_id, 2)

    def test_negative_idx_not_mapped_to_other_namespace(self):
        bulk = self._make_bulk(
            ["db.coll1", "db.coll2", "db.coll1"],
            verbose_results=True,
            max_write_batch_size=2,
        )
        replies = [
            self._reply([{"idx": 0, "ok": 1}, {"idx": 1, "ok": 1}], n_inserted=2),
            # idx -1 would silently wrap around to the previous batch's
            # db.coll2 operation.
            self._reply([{"idx": -1, "ok": 1}], n_inserted=1),
        ]
        with self.assertRaises(ClientBulkWriteException) as context:
            self._run_bulk(bulk, replies)
        self.assertIn("idx", context.exception.error["errmsg"])
        self.assertEqual(context.exception.write_errors, [])
        # The first batch's results are preserved as a partial result and
        # the invalid idx neither overwrote them nor was reported as success.
        partial = context.exception.partial_result
        self.assertIsNotNone(partial)
        self.assertEqual(set(partial.insert_results), {0, 1})
        self.assertEqual(partial.insert_results[1].inserted_id, 1)

    def test_out_of_range_idx_not_mapped_to_future_batch(self):
        bulk = self._make_bulk(
            ["db.coll1", "db.coll2", "db.coll1"],
            verbose_results=True,
            max_write_batch_size=2,
        )
        replies = [
            # idx 2 belongs to the next batch (a different namespace) and
            # must not be accepted for this two-operation batch.
            self._reply([{"idx": 0, "ok": 1}, {"idx": 2, "ok": 1}], n_inserted=2),
            self._reply([{"idx": 0, "ok": 1}], n_inserted=1),
        ]
        with self.assertRaises(ClientBulkWriteException) as context:
            self._run_bulk(bulk, replies)
        self.assertIn("idx", context.exception.error["errmsg"])
        partial = context.exception.partial_result
        self.assertIsNotNone(partial)
        # Operation 2 had not been sent yet and must have no result.
        self.assertNotIn(2, partial.insert_results)

    def test_idx_beyond_total_operations_rejected(self):
        bulk = self._make_bulk(["db.coll1", "db.coll2"], max_write_batch_size=2)
        replies = [self._reply([{"idx": 0, "ok": 1}, {"idx": 100, "ok": 1}], n_inserted=2)]
        with self.assertRaises(ClientBulkWriteException) as context:
            self._run_bulk(bulk, replies)
        self.assertIn("idx", context.exception.error["errmsg"])

    def test_duplicate_idx_does_not_overwrite_result(self):
        bulk = self._make_bulk(
            ["db.coll1", "db.coll2"], verbose_results=True, max_write_batch_size=2
        )
        replies = [
            self._reply([{"idx": 0, "ok": 1}, {"idx": 0, "ok": 1}], n_inserted=2),
        ]
        with self.assertRaises(ClientBulkWriteException) as context:
            self._run_bulk(bulk, replies)
        self.assertIn("idx", context.exception.error["errmsg"])
        partial = context.exception.partial_result
        self.assertIsNotNone(partial)
        # The duplicate idx did not overwrite or suppress any result.
        self.assertEqual(set(partial.insert_results), {0})

    def test_missing_idx_rejected(self):
        bulk = self._make_bulk(["db.coll1"], max_write_batch_size=1)
        replies = [self._reply([{"ok": 1}], n_inserted=1)]
        with self.assertRaises(ClientBulkWriteException) as context:
            self._run_bulk(bulk, replies)
        self.assertIn("idx", context.exception.error["errmsg"])

    def test_ordered_halts_after_write_error(self):
        bulk = self._make_bulk(
            ["db.coll1", "db.coll2", "db.coll1"], ordered=True, max_write_batch_size=2
        )
        replies = [
            self._reply(
                [{"idx": 0, "ok": 1}, {"idx": 1, "ok": 0, "code": 123, "errmsg": "boom"}],
                n_inserted=1,
            ),
        ]
        with self.assertRaises(ClientBulkWriteException) as context:
            self._run_bulk(bulk, replies)
        # Execution halted after the first batch.
        self.assertEqual(len(self._sent_batches), 1)
        self.assertEqual(len(context.exception.write_errors), 1)
        write_error = context.exception.write_errors[0]
        self.assertEqual(write_error["idx"], 1)
        # The error is attributed to the db.coll2 operation (nsInfo index 1).
        self.assertEqual(write_error["op"], {"insert": 1, "document": {"_id": 1}})

    def test_unordered_maps_errors_to_correct_operations_across_batches(self):
        bulk = self._make_bulk(
            ["db.coll1", "db.coll2", "db.coll1"], ordered=False, max_write_batch_size=2
        )
        replies = [
            self._reply(
                [{"idx": 0, "ok": 1}, {"idx": 1, "ok": 0, "code": 123, "errmsg": "boom"}],
                n_inserted=1,
            ),
            self._reply(
                [{"idx": 0, "ok": 0, "code": 456, "errmsg": "bang"}],
                n_inserted=0,
            ),
        ]
        with self.assertRaises(ClientBulkWriteException) as context:
            self._run_bulk(bulk, replies)
        self.assertEqual(len(self._sent_batches), 2)
        write_errors = context.exception.write_errors
        self.assertEqual(len(write_errors), 2)
        # Each error is mapped back to the correct original operation.
        self.assertEqual(write_errors[0]["idx"], 1)
        self.assertEqual(write_errors[0]["op"], {"insert": 1, "document": {"_id": 1}})
        self.assertEqual(write_errors[1]["idx"], 2)
        self.assertEqual(write_errors[1]["op"], {"insert": 0, "document": {"_id": 2}})
