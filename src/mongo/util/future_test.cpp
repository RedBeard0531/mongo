/**
 *    Copyright (C) 2018 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/util/future.h"

#include "mongo/stdx/thread.h"
#include "mongo/unittest/death_test.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {

template <typename T, typename Func>
void completePromise(Promise<T>* promise, Func&& func) {
    promise->emplaceValue(func());
}

template <typename Func>
void completePromise(Promise<void>* promise, Func&& func) {
    func();
    promise->emplaceValue();
}

template <typename Func, typename Result = std::result_of_t<Func && ()>>
Future<Result> async(Func&& func) {
    Promise<Result> promise;
    auto fut = promise.getFuture();

    stdx::thread([ promise = std::move(promise), func = std::forward<Func>(func) ]() mutable {
        sleepmillis(100);  // Try to wait until after the Future has been handled.
        try {
            completePromise(&promise, func);
        } catch (const DBException& ex) {
            promise.setError(ex.toStatus());
        }
    }).detach();

    return fut;
}

const auto failStatus = Status(ErrorCodes::Error(50716), "expected failure");

#define ASSERT_THROWS_failStatus(expr)                                          \
    [&] {                                                                       \
        ASSERT_THROWS_WITH_CHECK(expr, DBException, [](const DBException& ex) { \
            ASSERT_EQ(ex.toStatus(), failStatus);                               \
        });                                                                     \
    }()

// Tests a Future completed by completionExpr using testFunc. The Future will be completed in
// various ways to maximize test coverage.
#define FUTURE_SUCCESS_TEST(CompletionType, completionExpr, testFunc)                             \
    [&] {                                                                                         \
        auto testFuncVal = (testFunc);                                                            \
        { /* immediate future */                                                                  \
            testFuncVal(Future<CompletionType>::makeReady(completionExpr));                       \
        }                                                                                         \
        { /* ready future from promise */                                                         \
            Promise<CompletionType> promise;                                                      \
            auto fut = promise.getFuture(); /* before setting value to bypass opt to immediate */ \
            promise.emplaceValue(completionExpr);                                                 \
            testFuncVal(std::move(fut));                                                          \
        }                                                                                         \
                                                                                                  \
        { /* async future */                                                                      \
            testFuncVal(async([&] { return completionExpr; }));                                   \
        }                                                                                         \
    }()

#define FUTURE_FAIL_TEST(CompletionType, testFunc)                                                \
    [&] {                                                                                         \
        auto testFuncVal = (testFunc);                                                            \
        { /* immediate future */                                                                  \
            testFuncVal(Future<CompletionType>::makeReady(failStatus));                           \
        }                                                                                         \
        { /* ready future from promise */                                                         \
            Promise<CompletionType> promise;                                                      \
            auto fut = promise.getFuture(); /* before setting value to bypass opt to immediate */ \
            promise.setError(failStatus);                                                         \
            testFuncVal(std::move(fut));                                                          \
        }                                                                                         \
                                                                                                  \
        { /* async future */                                                                      \
            testFuncVal(async([&]() -> CompletionType {                                           \
                uassertStatusOK(failStatus);                                                      \
                MONGO_UNREACHABLE;                                                                \
            }));                                                                                  \
        }                                                                                         \
    }()

TEST(Future, Success_getLvalue) {
    FUTURE_SUCCESS_TEST(int, 1, [](Future<int>&& fut) { ASSERT_EQ(fut.get(), 1); });
}

TEST(Future, Success_getConstLvalue) {
    FUTURE_SUCCESS_TEST(int, 1, [](const Future<int>& fut) { ASSERT_EQ(fut.get(), 1); });
}

TEST(Future, Success_getRvalue) {
    FUTURE_SUCCESS_TEST(int, 1, [](Future<int>&& fut) { ASSERT_EQ(std::move(fut).get(), 1); });
}

TEST(Future, Success_getNothrowLvalue) {
    FUTURE_SUCCESS_TEST(int, 1, [](Future<int>&& fut) { ASSERT_EQ(fut.getNoThrow(), 1); });
}

TEST(Future, Success_getNothrowConstLvalue) {
    FUTURE_SUCCESS_TEST(int, 1, [](const Future<int>& fut) { ASSERT_EQ(fut.getNoThrow(), 1); });
}

TEST(Future, Success_getNothrowRvalue) {
    FUTURE_SUCCESS_TEST(
        int, 1, [](Future<int>&& fut) { ASSERT_EQ(std::move(fut).getNoThrow(), 1); });
}

TEST(Future, Success_getAsync) {
    FUTURE_SUCCESS_TEST(int, 1, [](Future<int>&& fut) {
        Promise<int> outside;
        std::move(fut).getAsync([&outside](StatusWith<int> sw) {
            ASSERT_OK(sw);
            outside.emplaceValue(sw.getValue());
        });
        ASSERT_EQ(outside.getFuture().get(), 1);
    });
}

TEST(Future, Fail_getLvalue) {
    FUTURE_FAIL_TEST(int, [](Future<int>&& fut) { ASSERT_THROWS_failStatus(fut.get()); });
}

TEST(Future, Fail_getConstLvalue) {
    FUTURE_FAIL_TEST(int, [](const Future<int>& fut) { ASSERT_THROWS_failStatus(fut.get()); });
}

TEST(Future, Fail_getRvalue) {
    FUTURE_FAIL_TEST(int,
                     [](Future<int>&& fut) { ASSERT_THROWS_failStatus(std::move(fut).get()); });
}

TEST(Future, Fail_getNothrowLvalue) {
    FUTURE_FAIL_TEST(int, [](Future<int>&& fut) { ASSERT_EQ(fut.getNoThrow(), failStatus); });
}

TEST(Future, Fail_getNothrowConstLvalue) {
    FUTURE_FAIL_TEST(int, [](const Future<int>& fut) { ASSERT_EQ(fut.getNoThrow(), failStatus); });
}

TEST(Future, Fail_getNothrowRvalue) {
    FUTURE_FAIL_TEST(int,
                     [](Future<int>&& fut) { ASSERT_EQ(std::move(fut).getNoThrow(), failStatus); });
}

TEST(Future, Fail_getAsync) {
    FUTURE_FAIL_TEST(int, [](Future<int>&& fut) {
        Promise<int> outside;
        std::move(fut).getAsync([&outside](StatusWith<int> sw) {
            ASSERT(!sw.isOK());
            outside.setError(sw.getStatus());
        });
        ASSERT_EQ(outside.getFuture().getNoThrow(), failStatus);
    });
}

TEST(Future, Success_thenSimple) {
    FUTURE_SUCCESS_TEST(int, 1, [](Future<int>&& fut) {
        ASSERT_EQ(std::move(fut).then([](int i) { return i + 2; }).get(), 3);
    });
}

TEST(Future, Success_thenVoid) {
    FUTURE_SUCCESS_TEST(int, 1, [](Future<int>&& fut) {
        ASSERT_EQ(std::move(fut).then([](int i) { ASSERT_EQ(i, 1); }).then([] { return 3; }).get(),
                  3);
    });
}

TEST(Future, Success_thenStatus) {
    FUTURE_SUCCESS_TEST(int, 1, [](Future<int>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .then([](int i) {
                          ASSERT_EQ(i, 1);
                          return Status::OK();
                      })
                      .then([] { return 3; })
                      .get(),
                  3);
    });
}

TEST(Future, Success_thenError_Status) {
    FUTURE_SUCCESS_TEST(int, 1, [](Future<int>&& fut) {
        auto fut2 =
            std::move(fut).then([](int i) { return Status(ErrorCodes::BadValue, "oh no!"); });
        MONGO_STATIC_ASSERT(std::is_same<decltype(fut2), Future<void>>::value);
        ASSERT_THROWS(fut2.get(), ExceptionFor<ErrorCodes::BadValue>);
    });
}

TEST(Future, Success_thenError_StatusWith) {
    FUTURE_SUCCESS_TEST(int, 1, [](Future<int>&& fut) {
        auto fut2 = std::move(fut).then(
            [](int i) { return StatusWith<double>(ErrorCodes::BadValue, "oh no!"); });
        MONGO_STATIC_ASSERT(std::is_same<decltype(fut2), Future<double>>::value);
        ASSERT_THROWS(fut2.get(), ExceptionFor<ErrorCodes::BadValue>);
    });
}

TEST(Future, Success_thenFutureImmediate) {
    FUTURE_SUCCESS_TEST(int, 1, [](Future<int>&& fut) {
        ASSERT_EQ(std::move(fut).then([](int i) { return Future<int>::makeReady(i + 2); }).get(),
                  3);
    });
}

TEST(Future, Success_thenFutureReady) {
    FUTURE_SUCCESS_TEST(int, 1, [](Future<int>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .then([](int i) {
                          Promise<int> promise;
                          auto fut = promise.getFuture();
                          promise.emplaceValue(i + 2);
                          return fut;
                      })
                      .get(),
                  3);
    });
}

TEST(Future, Success_thenFutureAsync) {
    FUTURE_SUCCESS_TEST(int, 1, [](Future<int>&& fut) {
        ASSERT_EQ(std::move(fut).then([](int i) { return async([i] { return i + 2; }); }).get(), 3);
    });
}

TEST(Future, Success_thenFutureAsyncThrow) {
    FUTURE_SUCCESS_TEST(int, 1, [](Future<int>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .then([](int i) {
                          uasserted(ErrorCodes::BadValue, "oh no!");
                          return Future<int>();
                      })
                      .getNoThrow(),
                  ErrorCodes::BadValue);
    });
}

TEST(Future, Fail_thenSimple) {
    FUTURE_FAIL_TEST(int, [](Future<int>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .then([](int i) {
                          FAIL("then() callback was called");
                          return int();
                      })
                      .getNoThrow(),
                  failStatus);
    });
}

TEST(Future, Fail_thenFutureAsync) {
    FUTURE_FAIL_TEST(int, [](Future<int>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .then([](int i) {
                          FAIL("then() callback was called");
                          return Future<int>();
                      })
                      .getNoThrow(),
                  failStatus);
    });
}

TEST(Future, Success_onErrorSimple) {
    FUTURE_SUCCESS_TEST(int, 1, [](Future<int>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .onError([](Status) {
                          FAIL("onError() callback was called");
                          return 0;
                      })
                      .then([](int i) { return i + 2; })
                      .get(),
                  3);
    });
}

TEST(Future, Success_onErrorFutureAsync) {
    FUTURE_SUCCESS_TEST(int, 1, [](Future<int>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .onError([](Status) {
                          FAIL("onError() callback was called");
                          return Future<int>();
                      })
                      .then([](int i) { return i + 2; })
                      .get(),
                  3);
    });
}

TEST(Future, Fail_onErrorSimple) {
    FUTURE_FAIL_TEST(int, [](Future<int>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .onError([](Status s) {
                          ASSERT_EQ(s, failStatus);
                          return 3;
                      })
                      .getNoThrow(),
                  3);
    });
}

TEST(Future, Fail_onErrorError_throw) {
    FUTURE_FAIL_TEST(int, [](Future<int>&& fut) {
        auto fut2 = std::move(fut).onError([](Status s) -> int {
            ASSERT_EQ(s, failStatus);
            uasserted(ErrorCodes::BadValue, "oh no!");
        });
        ASSERT_EQ(fut2.getNoThrow(), ErrorCodes::BadValue);
    });
}

TEST(Future, Fail_onErrorError_StatusWith) {
    FUTURE_FAIL_TEST(int, [](Future<int>&& fut) {
        auto fut2 = std::move(fut).onError([](Status s) {
            ASSERT_EQ(s, failStatus);
            return StatusWith<int>(ErrorCodes::BadValue, "oh no!");
        });
        ASSERT_EQ(fut2.getNoThrow(), ErrorCodes::BadValue);
    });
}

TEST(Future, Fail_onErrorFutureImmediate) {
    FUTURE_FAIL_TEST(int, [](Future<int>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .onError([](Status s) {
                          ASSERT_EQ(s, failStatus);
                          return Future<int>::makeReady(3);
                      })
                      .get(),
                  3);
    });
}

TEST(Future, Fail_onErrorFutureReady) {
    FUTURE_FAIL_TEST(int, [](Future<int>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .onError([](Status s) {
                          ASSERT_EQ(s, failStatus);
                          Promise<int> promise;
                          auto fut = promise.getFuture();
                          promise.emplaceValue(3);
                          return fut;
                      })
                      .get(),
                  3);
    });
}

TEST(Future, Fail_onErrorFutureAsync) {
    FUTURE_FAIL_TEST(int, [](Future<int>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .onError([](Status s) {
                          ASSERT_EQ(s, failStatus);
                          return async([] { return 3; });
                      })
                      .get(),
                  3);
    });
}

TEST(Future, Success_tap) {
    FUTURE_SUCCESS_TEST(int, 1, [](Future<int>&& fut) {
        bool tapCalled = false;
        ASSERT_EQ(std::move(fut)
                      .tap([&tapCalled](int i) {
                          ASSERT_EQ(i, 1);
                          tapCalled = true;
                      })
                      .then([](int i) { return i + 2; })
                      .get(),
                  3);
        ASSERT(tapCalled);
    });
}

TEST(Future, Success_tapError) {
    FUTURE_SUCCESS_TEST(int, 1, [](Future<int>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .tapError([](Status s) { FAIL("tapError() callback was called"); })
                      .then([](int i) { return i + 2; })
                      .get(),
                  3);
    });
}

TEST(Future, Success_tapAll_StatusWith) {
    FUTURE_SUCCESS_TEST(int, 1, [](Future<int>&& fut) {
        bool tapCalled = false;
        ASSERT_EQ(std::move(fut)
                      .tapAll([&tapCalled](StatusWith<int> sw) {
                          ASSERT_EQ(sw, 1);
                          tapCalled = true;
                      })
                      .then([](int i) { return i + 2; })
                      .get(),
                  3);
        ASSERT(tapCalled);
    });
}

TEST(Future, Success_tapAll_Overloaded) {
    FUTURE_SUCCESS_TEST(int, 1, [](Future<int>&& fut) {
        struct Callback {
            void operator()(int i) {
                ASSERT_EQ(i, 1);
                called = true;
            }
            void operator()(Status status) {
                FAIL("Status overload called with ") << status;
            }
            bool called = false;
        };
        Callback callback;

        ASSERT_EQ(std::move(fut).tapAll(std::ref(callback)).then([](int i) { return i + 2; }).get(),
                  3);
        ASSERT(callback.called);
    });
}

TEST(Future, Fail_tap) {
    FUTURE_FAIL_TEST(int, [](Future<int>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .tap([](int i) { FAIL("tap() callback was called"); })
                      .onError([](Status s) {
                          ASSERT_EQ(s, failStatus);
                          return 3;
                      })
                      .get(),
                  3);
    });
}

TEST(Future, Fail_tapError) {
    FUTURE_FAIL_TEST(int, [](Future<int>&& fut) {
        bool tapCalled = false;
        ASSERT_EQ(std::move(fut)
                      .tapError([&tapCalled](Status s) {
                          ASSERT_EQ(s, failStatus);
                          tapCalled = true;
                      })
                      .onError([](Status s) {
                          ASSERT_EQ(s, failStatus);
                          return 3;
                      })
                      .get(),
                  3);
        ASSERT(tapCalled);
    });
}

TEST(Future, Fail_tapAll_StatusWith) {
    FUTURE_FAIL_TEST(int, [](Future<int>&& fut) {
        bool tapCalled = false;
        ASSERT_EQ(std::move(fut)
                      .tapAll([&tapCalled](StatusWith<int> sw) {
                          ASSERT_EQ(sw.getStatus(), failStatus);
                          tapCalled = true;
                      })
                      .onError([](Status s) {
                          ASSERT_EQ(s, failStatus);
                          return 3;
                      })
                      .get(),
                  3);
        ASSERT(tapCalled);
    });
}

TEST(Future, Fail_tapAll_Overloaded) {
    FUTURE_FAIL_TEST(int, [](Future<int>&& fut) {
        struct Callback {
            void operator()(int i) {
                FAIL("int overload called with ") << i;
            }
            void operator()(Status status) {
                ASSERT_EQ(status, failStatus);
                called = true;
            }
            bool called = false;
        };
        Callback callback;

        ASSERT_EQ(std::move(fut)
                      .tapAll(std::ref(callback))
                      .onError([](Status s) {
                          ASSERT_EQ(s, failStatus);
                          return 3;
                      })
                      .get(),
                  3);

        ASSERT(callback.called);
    });
}

TEST(Future_Void, Success_getLvalue) {
    FUTURE_SUCCESS_TEST(void, , [](Future<void>&& fut) { fut.get(); });
}

TEST(Future_Void, Success_getConstLvalue) {
    FUTURE_SUCCESS_TEST(void, , [](const Future<void>& fut) { fut.get(); });
}

TEST(Future_Void, Success_getRvalue) {
    FUTURE_SUCCESS_TEST(void, , [](Future<void>&& fut) { std::move(fut).get(); });
}

TEST(Future_Void, Success_getNothrowLvalue) {
    FUTURE_SUCCESS_TEST(
        void, , [](Future<void>&& fut) { ASSERT_EQ(fut.getNoThrow(), Status::OK()); });
}

TEST(Future_Void, Success_getNothrowConstLvalue) {
    FUTURE_SUCCESS_TEST(
        void, , [](const Future<void>& fut) { ASSERT_EQ(fut.getNoThrow(), Status::OK()); });
}

TEST(Future_Void, Success_getNothrowRvalue) {
    FUTURE_SUCCESS_TEST(
        void, , [](Future<void>&& fut) { ASSERT_EQ(std::move(fut).getNoThrow(), Status::OK()); });
}

TEST(Future_Void, Success_getAsync) {
    FUTURE_SUCCESS_TEST(void, , [](Future<void>&& fut) {
        Promise<void> outside;
        std::move(fut).getAsync([&outside](Status status) {
            ASSERT_OK(status);
            outside.emplaceValue();
        });
        ASSERT_EQ(outside.getFuture().getNoThrow(), Status::OK());
    });
}

TEST(Future_Void, Fail_getLvalue) {
    FUTURE_FAIL_TEST(void, [](Future<void>&& fut) { ASSERT_THROWS_failStatus(fut.get()); });
}

TEST(Future_Void, Fail_getConstLvalue) {
    FUTURE_FAIL_TEST(void, [](const Future<void>& fut) { ASSERT_THROWS_failStatus(fut.get()); });
}

TEST(Future_Void, Fail_getRvalue) {
    FUTURE_FAIL_TEST(void,
                     [](Future<void>&& fut) { ASSERT_THROWS_failStatus(std::move(fut).get()); });
}

TEST(Future_Void, Fail_getNothrowLvalue) {
    FUTURE_FAIL_TEST(void, [](Future<void>&& fut) { ASSERT_EQ(fut.getNoThrow(), failStatus); });
}

TEST(Future_Void, Fail_getNothrowConstLvalue) {
    FUTURE_FAIL_TEST(void,
                     [](const Future<void>& fut) { ASSERT_EQ(fut.getNoThrow(), failStatus); });
}

TEST(Future_Void, Fail_getNothrowRvalue) {
    FUTURE_FAIL_TEST(
        void, [](Future<void>&& fut) { ASSERT_EQ(std::move(fut).getNoThrow(), failStatus); });
}

TEST(Future_Void, Fail_getAsync) {
    FUTURE_FAIL_TEST(void, [](Future<void>&& fut) {
        Promise<void> outside;
        std::move(fut).getAsync([&outside](Status status) {
            ASSERT(!status.isOK());
            outside.setError(status);
        });
        ASSERT_EQ(outside.getFuture().getNoThrow(), failStatus);
    });
}

TEST(Future_Void, Success_thenSimple) {
    FUTURE_SUCCESS_TEST(void, , [](Future<void>&& fut) {
        ASSERT_EQ(std::move(fut).then([]() { return 3; }).get(), 3);
    });
}

TEST(Future_Void, Success_thenVoid) {
    FUTURE_SUCCESS_TEST(void, , [](Future<void>&& fut) {
        ASSERT_EQ(std::move(fut).then([] {}).then([] { return 3; }).get(), 3);
    });
}

TEST(Future_Void, Success_thenStatus) {
    FUTURE_SUCCESS_TEST(void, , [](Future<void>&& fut) {
        ASSERT_EQ(std::move(fut).then([] {}).then([] { return 3; }).get(), 3);
    });
}

TEST(Future_Void, Success_thenError_Status) {
    FUTURE_SUCCESS_TEST(void, , [](Future<void>&& fut) {
        auto fut2 = std::move(fut).then([]() { return Status(ErrorCodes::BadValue, "oh no!"); });
        MONGO_STATIC_ASSERT(std::is_same<decltype(fut2), Future<void>>::value);
        ASSERT_EQ(fut2.getNoThrow(), ErrorCodes::BadValue);
    });
}

TEST(Future_Void, Success_thenError_StatusWith) {
    FUTURE_SUCCESS_TEST(void, , [](Future<void>&& fut) {
        auto fut2 = std::move(fut).then(
            []() { return StatusWith<double>(ErrorCodes::BadValue, "oh no!"); });
        MONGO_STATIC_ASSERT(std::is_same<decltype(fut2), Future<double>>::value);
        ASSERT_EQ(fut2.getNoThrow(), ErrorCodes::BadValue);
    });
}

TEST(Future_Void, Success_thenFutureImmediate) {
    FUTURE_SUCCESS_TEST(void, , [](Future<void>&& fut) {
        ASSERT_EQ(std::move(fut).then([]() { return Future<int>::makeReady(3); }).get(), 3);
    });
}

TEST(Future_Void, Success_thenFutureReady) {
    FUTURE_SUCCESS_TEST(void, , [](Future<void>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .then([]() {
                          Promise<int> promise;
                          auto fut = promise.getFuture();
                          promise.emplaceValue(3);
                          return fut;
                      })
                      .get(),
                  3);
    });
}

TEST(Future_Void, Success_thenFutureAsync) {
    FUTURE_SUCCESS_TEST(void, , [](Future<void>&& fut) {
        ASSERT_EQ(std::move(fut).then([]() { return async([] { return 3; }); }).get(), 3);
    });
}

TEST(Future_Void, Fail_thenSimple) {
    FUTURE_FAIL_TEST(void, [](Future<void>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .then([]() {
                          FAIL("then() callback was called");
                          return int();
                      })
                      .getNoThrow(),
                  failStatus);
    });
}

TEST(Future_Void, Fail_thenFutureAsync) {
    FUTURE_FAIL_TEST(void, [](Future<void>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .then([]() {
                          FAIL("then() callback was called");
                          return Future<int>();
                      })
                      .getNoThrow(),
                  failStatus);
    });
}

TEST(Future_Void, Success_onErrorSimple) {
    FUTURE_SUCCESS_TEST(void, , [](Future<void>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .onError([](Status) { FAIL("onError() callback was called"); })
                      .then([] { return 3; })
                      .get(),
                  3);
    });
}

TEST(Future_Void, Success_onErrorFutureAsync) {
    FUTURE_SUCCESS_TEST(void, , [](Future<void>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .onError([](Status) {
                          FAIL("onError() callback was called");
                          return Future<void>();
                      })
                      .then([] { return 3; })
                      .get(),
                  3);
    });
}

TEST(Future_Void, Fail_onErrorSimple) {
    FUTURE_FAIL_TEST(void, [](Future<void>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .onError([](Status s) { ASSERT_EQ(s, failStatus); })
                      .then([] { return 3; })
                      .getNoThrow(),
                  3);
    });
}
TEST(Future_Void, Fail_onErrorError_throw) {
    FUTURE_FAIL_TEST(void, [](Future<void>&& fut) {
        auto fut2 = std::move(fut).onError([](Status s) {
            ASSERT_EQ(s, failStatus);
            uasserted(ErrorCodes::BadValue, "oh no!");
        });
        ASSERT_EQ(fut2.getNoThrow(), ErrorCodes::BadValue);
    });
}

TEST(Future_Void, Fail_onErrorError_Status) {
    FUTURE_FAIL_TEST(void, [](Future<void>&& fut) {
        auto fut2 = std::move(fut).onError([](Status s) {
            ASSERT_EQ(s, failStatus);
            return Status(ErrorCodes::BadValue, "oh no!");
        });
        ASSERT_EQ(fut2.getNoThrow(), ErrorCodes::BadValue);
    });
}

TEST(Future_Void, Fail_onErrorFutureImmediate) {
    FUTURE_FAIL_TEST(void, [](Future<void>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .onError([](Status s) {
                          ASSERT_EQ(s, failStatus);
                          return Future<void>::makeReady();
                      })
                      .then([] { return 3; })
                      .get(),
                  3);
    });
}

TEST(Future_Void, Fail_onErrorFutureReady) {
    FUTURE_FAIL_TEST(void, [](Future<void>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .onError([](Status s) {
                          ASSERT_EQ(s, failStatus);
                          Promise<void> promise;
                          auto fut = promise.getFuture();
                          promise.emplaceValue();
                          return fut;
                      })
                      .then([] { return 3; })
                      .get(),
                  3);
    });
}

TEST(Future_Void, Fail_onErrorFutureAsync) {
    FUTURE_FAIL_TEST(void, [](Future<void>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .onError([&](Status s) {
                          ASSERT_EQ(s, failStatus);
                          return async([] {});
                      })
                      .then([] { return 3; })
                      .get(),
                  3);
    });
}

TEST(Future_Void, Success_tap) {
    FUTURE_SUCCESS_TEST(void, , [](Future<void>&& fut) {
        bool tapCalled = false;
        ASSERT_EQ(
            std::move(fut).tap([&tapCalled] { tapCalled = true; }).then([] { return 3; }).get(), 3);
        ASSERT(tapCalled);
    });
}

TEST(Future_Void, Success_tapError) {
    FUTURE_SUCCESS_TEST(void, , [](Future<void>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .tapError([](Status s) { FAIL("tapError() callback was called"); })
                      .then([] { return 3; })
                      .get(),
                  3);
    });
}

TEST(Future_Void, Success_tapAll_StatusWith) {
    FUTURE_SUCCESS_TEST(void, , [](Future<void>&& fut) {
        bool tapCalled = false;
        ASSERT_EQ(std::move(fut)
                      .tapAll([&tapCalled](Status s) {
                          ASSERT_OK(s);
                          tapCalled = true;
                      })
                      .then([] { return 3; })
                      .get(),
                  3);
        ASSERT(tapCalled);
    });
}

TEST(Future_Void, Success_tapAll_Overloaded) {
    FUTURE_SUCCESS_TEST(void, , [](Future<void>&& fut) {
        struct Callback {
            void operator()() {
                called = true;
            }
            void operator()(Status status) {
                FAIL("Status overload called with ") << status;
            }
            bool called = false;
        };
        Callback callback;

        ASSERT_EQ(std::move(fut).tapAll(std::ref(callback)).then([] { return 3; }).get(), 3);
        ASSERT(callback.called);
    });
}

TEST(Future_Void, Fail_tap) {
    FUTURE_FAIL_TEST(void, [](Future<void>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .tap([] { FAIL("tap() callback was called"); })
                      .onError([](Status s) { ASSERT_EQ(s, failStatus); })
                      .then([] { return 3; })
                      .get(),
                  3);
    });
}

TEST(Future_Void, Fail_tapError) {
    FUTURE_FAIL_TEST(void, [](Future<void>&& fut) {
        bool tapCalled = false;
        ASSERT_EQ(std::move(fut)
                      .tapError([&tapCalled](Status s) {
                          ASSERT_EQ(s, failStatus);
                          tapCalled = true;
                      })
                      .onError([](Status s) { ASSERT_EQ(s, failStatus); })
                      .then([] { return 3; })
                      .get(),
                  3);
        ASSERT(tapCalled);
    });
}

TEST(Future_Void, Fail_tapAll_StatusWith) {
    FUTURE_FAIL_TEST(void, [](Future<void>&& fut) {
        bool tapCalled = false;
        ASSERT_EQ(std::move(fut)
                      .tapAll([&tapCalled](StatusWith<int> sw) {
                          ASSERT_EQ(sw.getStatus(), failStatus);
                          tapCalled = true;
                      })
                      .onError([](Status s) { ASSERT_EQ(s, failStatus); })
                      .then([] { return 3; })
                      .get(),
                  3);
        ASSERT(tapCalled);
    });
}

TEST(Future_Void, Fail_tapAll_Overloaded) {
    FUTURE_FAIL_TEST(void, [](Future<void>&& fut) {
        struct Callback {
            void operator()() {
                FAIL("() overload called");
            }
            void operator()(Status status) {
                ASSERT_EQ(status, failStatus);
                called = true;
            }
            bool called = false;
        };
        Callback callback;

        ASSERT_EQ(std::move(fut)
                      .tapAll(std::ref(callback))
                      .onError([](Status s) { ASSERT_EQ(s, failStatus); })
                      .then([] { return 3; })
                      .get(),
                  3);

        ASSERT(callback.called);
    });
}

// A move-only type that isn't default constructible. It has binary ops with int to make it easier
// to have a common format with the above tests.
struct Widget {
    explicit Widget(int val) : val(val) {}

    Widget(Widget&& other) : Widget(other.val) {}
    Widget& operator=(Widget&& other) {
        val = other.val;
        return *this;
    }

    Widget() = delete;
    Widget(const Widget&) = delete;
    Widget& operator=(const Widget&) = delete;

    Widget operator+(int i) const {
        return Widget(val + i);
    }

    bool operator==(int i) const {
        return val == i;
    }

    bool operator==(Widget w) const {
        return val == w.val;
    }

    int val;
};

std::ostream& operator<<(std::ostream& stream, const Widget& widget) {
    return stream << "Widget(" << widget.val << ')';
}

TEST(Future_MoveOnly, Success_getLvalue) {
    FUTURE_SUCCESS_TEST(Widget, Widget(1), [](Future<Widget>&& fut) { ASSERT_EQ(fut.get(), 1); });
}

TEST(Future_MoveOnly, Success_getConstLvalue) {
    FUTURE_SUCCESS_TEST(
        Widget, Widget(1), [](const Future<Widget>& fut) { ASSERT_EQ(fut.get(), 1); });
}

TEST(Future_MoveOnly, Success_getRvalue) {
    FUTURE_SUCCESS_TEST(
        Widget, Widget(1), [](Future<Widget>&& fut) { ASSERT_EQ(std::move(fut).get(), 1); });
}

#if 0  // Needs copy
TEST(Future_MoveOnly, Success_getNothrowLvalue) {
    FUTURE_SUCCESS_TEST(
        Widget, Widget(1), [](Future<Widget>&& fut) { ASSERT_EQ(fut.getNoThrow(), 1); });
}

TEST(Future_MoveOnly, Success_getNothrowConstLvalue) {
    FUTURE_SUCCESS_TEST(
        Widget, Widget(1), [](const Future<Widget>& fut) { ASSERT_EQ(fut.getNoThrow(), 1); });
}
#endif

TEST(Future_MoveOnly, Success_getNothrowRvalue) {
    FUTURE_SUCCESS_TEST(Widget, Widget(1), [](Future<Widget>&& fut) {
        ASSERT_EQ(uassertStatusOK(std::move(fut).getNoThrow()).val, 1);
    });
}

TEST(Future_MoveOnly, Success_getAsync) {
    FUTURE_SUCCESS_TEST(Widget, Widget(1), [](Future<Widget>&& fut) {
        Promise<Widget> outside;
        std::move(fut).getAsync([&outside](StatusWith<Widget> sw) {
            ASSERT_OK(sw);
            outside.emplaceValue(std::move(sw.getValue()));
        });
        ASSERT_EQ(outside.getFuture().get(), 1);
    });
}

TEST(Future_MoveOnly, Fail_getLvalue) {
    FUTURE_FAIL_TEST(Widget, [](Future<Widget>&& fut) { ASSERT_THROWS_failStatus(fut.get()); });
}

TEST(Future_MoveOnly, Fail_getConstLvalue) {
    FUTURE_FAIL_TEST(Widget,
                     [](const Future<Widget>& fut) { ASSERT_THROWS_failStatus(fut.get()); });
}

TEST(Future_MoveOnly, Fail_getRvalue) {
    FUTURE_FAIL_TEST(Widget,
                     [](Future<Widget>&& fut) { ASSERT_THROWS_failStatus(std::move(fut).get()); });
}

#if 0  // Needs copy
TEST(Future_MoveOnly, Fail_getNothrowLvalue) {
    FUTURE_FAIL_TEST(Widget, [](Future<Widget>&& fut) { ASSERT_EQ(fut.getNoThrow(), failStatus); });
}

TEST(Future_MoveOnly, Fail_getNothrowConstLvalue) {
    FUTURE_FAIL_TEST(Widget,
                     [](const Future<Widget>& fut) { ASSERT_EQ(fut.getNoThrow(), failStatus); });
}
#endif

TEST(Future_MoveOnly, Fail_getNothrowRvalue) {
    FUTURE_FAIL_TEST(Widget, [](Future<Widget>&& fut) {
        ASSERT_EQ(std::move(fut).getNoThrow().getStatus(), failStatus);
    });
}

TEST(Future_MoveOnly, Fail_getAsync) {
    FUTURE_FAIL_TEST(Widget, [](Future<Widget>&& fut) {
        Promise<Widget> outside;
        std::move(fut).getAsync([&outside](StatusWith<Widget> sw) {
            ASSERT(!sw.isOK());
            outside.setError(sw.getStatus());
        });
        ASSERT_EQ(outside.getFuture().getNoThrow(), failStatus);
    });
}

TEST(Future_MoveOnly, Success_thenSimple) {
    FUTURE_SUCCESS_TEST(Widget, Widget(1), [](Future<Widget>&& fut) {
        ASSERT_EQ(std::move(fut).then([](Widget i) { return i + 2; }).get(), 3);
    });
}

TEST(Future_MoveOnly, Success_thenVoid) {
    FUTURE_SUCCESS_TEST(Widget, Widget(1), [](Future<Widget>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .then([](Widget i) { ASSERT_EQ(i, 1); })
                      .then([] { return Widget(3); })
                      .get(),
                  3);
    });
}

TEST(Future_MoveOnly, Success_thenStatus) {
    FUTURE_SUCCESS_TEST(Widget, Widget(1), [](Future<Widget>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .then([](Widget i) {
                          ASSERT_EQ(i, 1);
                          return Status::OK();
                      })
                      .then([] { return Widget(3); })
                      .get(),
                  3);
    });
}

TEST(Future_MoveOnly, Success_thenError_Status) {
    FUTURE_SUCCESS_TEST(Widget, Widget(1), [](Future<Widget>&& fut) {
        auto fut2 =
            std::move(fut).then([](Widget i) { return Status(ErrorCodes::BadValue, "oh no!"); });
        MONGO_STATIC_ASSERT(std::is_same<decltype(fut2), Future<void>>::value);
        ASSERT_EQ(fut2.getNoThrow(), ErrorCodes::BadValue);
    });
}

TEST(Future_MoveOnly, Success_thenError_StatusWith) {
    FUTURE_SUCCESS_TEST(Widget, Widget(1), [](Future<Widget>&& fut) {
        auto fut2 = std::move(fut).then(
            [](Widget i) { return StatusWith<double>(ErrorCodes::BadValue, "oh no!"); });
        MONGO_STATIC_ASSERT(std::is_same<decltype(fut2), Future<double>>::value);
        ASSERT_EQ(fut2.getNoThrow(), ErrorCodes::BadValue);
    });
}

TEST(Future_MoveOnly, Success_thenFutureImmediate) {
    FUTURE_SUCCESS_TEST(Widget, Widget(1), [](Future<Widget>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .then([](Widget i) { return Future<Widget>::makeReady(Widget(i + 2)); })
                      .get(),
                  3);
    });
}

TEST(Future_MoveOnly, Success_thenFutureReady) {
    FUTURE_SUCCESS_TEST(Widget, Widget(1), [](Future<Widget>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .then([](Widget i) {
                          Promise<Widget> promise;
                          auto fut = promise.getFuture();
                          promise.emplaceValue(i + 2);
                          return fut;
                      })
                      .get(),
                  3);
    });
}

TEST(Future_MoveOnly, Success_thenFutureAsync) {
    FUTURE_SUCCESS_TEST(Widget, Widget(1), [](Future<Widget>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .then([&](Widget i) { return async([i = i.val] { return Widget(i + 2); }); })
                      .get(),
                  3);
    });
}

TEST(Future_MoveOnly, Success_thenFutureAsyncThrow) {
    FUTURE_SUCCESS_TEST(Widget, Widget(1), [](Future<Widget>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .then([](Widget i) {
                          uasserted(ErrorCodes::BadValue, "oh no!");
                          return Future<Widget>();
                      })
                      .getNoThrow(),
                  ErrorCodes::BadValue);
    });
}

TEST(Future_MoveOnly, Fail_thenSimple) {
    FUTURE_FAIL_TEST(Widget, [](Future<Widget>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .then([](Widget i) {
                          FAIL("then() callback was called");
                          return Widget(0);
                      })
                      .getNoThrow(),
                  failStatus);
    });
}

TEST(Future_MoveOnly, Fail_thenFutureAsync) {
    FUTURE_FAIL_TEST(Widget, [](Future<Widget>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .then([](Widget i) {
                          FAIL("then() callback was called");
                          return Future<Widget>();
                      })
                      .getNoThrow(),
                  failStatus);
    });
}

TEST(Future_MoveOnly, Success_onErrorSimple) {
    FUTURE_SUCCESS_TEST(Widget, Widget(1), [](Future<Widget>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .onError([](Status) {
                          FAIL("onError() callback was called");
                          return Widget(0);
                      })
                      .then([](Widget i) { return i + 2; })
                      .get(),
                  3);
    });
}

TEST(Future_MoveOnly, Success_onErrorFutureAsync) {
    FUTURE_SUCCESS_TEST(Widget, Widget(1), [](Future<Widget>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .onError([](Status) {
                          FAIL("onError() callback was called");
                          return Future<Widget>();
                      })
                      .then([](Widget i) { return i + 2; })
                      .get(),
                  3);
    });
}

TEST(Future_MoveOnly, Fail_onErrorSimple) {
    FUTURE_FAIL_TEST(Widget, [](Future<Widget>&& fut) {
        ASSERT_EQ(uassertStatusOK(std::move(fut)
                                      .onError([](Status s) {
                                          ASSERT_EQ(s, failStatus);
                                          return Widget(3);
                                      })
                                      .getNoThrow()),
                  3);
    });
}
TEST(Future_MoveOnly, Fail_onErrorError_throw) {
    FUTURE_FAIL_TEST(Widget, [](Future<Widget>&& fut) {
        auto fut2 = std::move(fut).onError([](Status s) -> Widget {
            ASSERT_EQ(s, failStatus);
            uasserted(ErrorCodes::BadValue, "oh no!");
        });
        ASSERT_EQ(std::move(fut2).getNoThrow(), ErrorCodes::BadValue);
    });
}

TEST(Future_MoveOnly, Fail_onErrorError_StatusWith) {
    FUTURE_FAIL_TEST(Widget, [](Future<Widget>&& fut) {
        auto fut2 = std::move(fut).onError([](Status s) {
            ASSERT_EQ(s, failStatus);
            return StatusWith<Widget>(ErrorCodes::BadValue, "oh no!");
        });
        ASSERT_EQ(std::move(fut2).getNoThrow(), ErrorCodes::BadValue);
    });
}

TEST(Future_MoveOnly, Fail_onErrorFutureImmediate) {
    FUTURE_FAIL_TEST(Widget, [](Future<Widget>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .onError([](Status s) {
                          ASSERT_EQ(s, failStatus);
                          return Future<Widget>::makeReady(Widget(3));
                      })
                      .get(),
                  3);
    });
}

TEST(Future_MoveOnly, Fail_onErrorFutureReady) {
    FUTURE_FAIL_TEST(Widget, [](Future<Widget>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .onError([](Status s) {
                          ASSERT_EQ(s, failStatus);
                          Promise<Widget> promise;
                          auto fut = promise.getFuture();
                          promise.emplaceValue(3);
                          return fut;
                      })
                      .get(),
                  3);
    });
}

TEST(Future_MoveOnly, Fail_onErrorFutureAsync) {
    FUTURE_FAIL_TEST(Widget, [](Future<Widget>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .onError([&](Status s) {
                          ASSERT_EQ(s, failStatus);
                          return async([] { return Widget(3); });
                      })
                      .get(),
                  3);
    });
}

TEST(Future_MoveOnly, Success_tap) {
    FUTURE_SUCCESS_TEST(Widget, Widget(1), [](Future<Widget>&& fut) {
        bool tapCalled = false;
        ASSERT_EQ(std::move(fut)
                      .tap([&tapCalled](const Widget& i) {
                          ASSERT_EQ(i, 1);
                          tapCalled = true;
                      })
                      .then([](Widget i) { return i + 2; })
                      .get(),
                  3);
        ASSERT(tapCalled);
    });
}

TEST(Future_MoveOnly, Success_tapError) {
    FUTURE_SUCCESS_TEST(Widget, Widget(1), [](Future<Widget>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .tapError([](Status s) { FAIL("tapError() callback was called"); })
                      .then([](Widget i) { return i + 2; })
                      .get(),
                  3);
    });
}

#if 0  // Needs copy
TEST(Future_MoveOnly, Success_tapAll_StatusWith) {
    FUTURE_SUCCESS_TEST(Widget, Widget(1), [](Future<Widget>&& fut) {
        bool tapCalled = false;
        ASSERT_EQ(std::move(fut)
                      .tapAll([&tapCalled](StatusWith<Widget> sw) {
                          ASSERT_EQ(uassertStatusOK(sw).val, 1);
                          tapCalled = true;
                      })
                      .then([](Widget i) { return i + 2; })
                      .get(),
                  3);
        ASSERT(tapCalled);
    });
}
#endif

TEST(Future_MoveOnly, Success_tapAll_Overloaded) {
    FUTURE_SUCCESS_TEST(Widget, Widget(1), [](Future<Widget>&& fut) {
        struct Callback {
            void operator()(const Widget& i) {
                ASSERT_EQ(i, 1);
                called = true;
            }
            void operator()(Status status) {
                FAIL("Status overload called with ") << status;
            }
            bool called = false;
        };
        Callback callback;

        ASSERT_EQ(
            std::move(fut).tapAll(std::ref(callback)).then([](Widget i) { return i + 2; }).get(),
            3);
        ASSERT(callback.called);
    });
}

TEST(Future_MoveOnly, Fail_tap) {
    FUTURE_FAIL_TEST(Widget, [](Future<Widget>&& fut) {
        ASSERT_EQ(std::move(fut)
                      .tap([](const Widget& i) { FAIL("tap() callback was called"); })
                      .onError([](Status s) {
                          ASSERT_EQ(s, failStatus);
                          return Widget(3);
                      })
                      .get(),
                  3);
    });
}

TEST(Future_MoveOnly, Fail_tapError) {
    FUTURE_FAIL_TEST(Widget, [](Future<Widget>&& fut) {
        bool tapCalled = false;
        ASSERT_EQ(std::move(fut)
                      .tapError([&tapCalled](Status s) {
                          ASSERT_EQ(s, failStatus);
                          tapCalled = true;
                      })
                      .onError([](Status s) {
                          ASSERT_EQ(s, failStatus);
                          return Widget(3);
                      })
                      .get(),
                  3);
        ASSERT(tapCalled);
    });
}

#if 0  // Needs copy
TEST(Future_MoveOnly, Fail_tapAll_StatusWith) {
    FUTURE_FAIL_TEST(Widget, [](Future<Widget>&& fut) {
        bool tapCalled = false;
        ASSERT_EQ(std::move(fut)
                      .tapAll([&tapCalled](StatusWith<Widget> sw) {
                          ASSERT_EQ(sw.getStatus(), failStatus);
                          tapCalled = true;
                      })
                      .onError([](Status s) {
                          ASSERT_EQ(s, failStatus);
                          return Widget(3);
                      })
                      .get(),
                  3);
        ASSERT(tapCalled);
    });
}
#endif

TEST(Future_MoveOnly, Fail_tapAll_Overloaded) {
    FUTURE_FAIL_TEST(Widget, [](Future<Widget>&& fut) {
        struct Callback {
            void operator()(const Widget& i) {
                FAIL("Widget overload called with ") << i;
            }
            void operator()(Status status) {
                ASSERT_EQ(status, failStatus);
                called = true;
            }
            bool called = false;
        };
        Callback callback;

        ASSERT_EQ(std::move(fut)
                      .tapAll(std::ref(callback))
                      .onError([](Status s) {
                          ASSERT_EQ(s, failStatus);
                          return Widget(3);
                      })
                      .get(),
                  3);

        ASSERT(callback.called);
    });
}

// This is the motivating case for SharedStateBase::isJustForContinuation. Without that logic, there
// would be a long chain of SharedStates, growing longer with each recursion. That logic exists to
// limit it to a fixed-size chain.
TEST(Future_EdgeCases, looping_onError) {
    int tries = 10;
    std::function<Future<int>()> read = [&] {
        return async([&] {
                   uassert(ErrorCodes::BadValue, "", --tries == 0);
                   return tries;
               })
            .onError([&](Status) { return read(); });
    };
    ASSERT_EQ(read().get(), 0);
}

// This tests for a bug in an earlier implementation of isJustForContinuation. Due to an off-by-one,
// it would replace the "then" continuation's SharedState. A different type is used for the return
// from then to cause it to fail a checked_cast close to the bug in debug builds.
TEST(Future_EdgeCases, looping_onError_with_then) {
    int tries = 10;
    std::function<Future<int>()> read = [&] {
        return async([&] {
                   uassert(ErrorCodes::BadValue, "", --tries == 0);
                   return tries;
               })
            .onError([&](Status) { return read(); });
    };
    ASSERT_EQ(read().then([](int x) { return x + 0.5; }).get(), 0.5);
}

// Make sure we actually die if someone throws from the getAsync callback.
//
// With gcc 5.8 we terminate, but print "terminate() called. No exception is active". This works in
// clang and gcc 7, so hopefully we can change the death-test search string to "die die die!!!" when
// we upgrade the toolchain.
DEATH_TEST(Future_EdgeCases, Success_getAsync_throw, "terminate() called") {
    Future<void>::makeReady().getAsync(
        [](Status) { uasserted(ErrorCodes::BadValue, "die die die!!!"); });
}

}  // namespace
}  // namespace mongo
