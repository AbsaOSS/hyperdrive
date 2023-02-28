/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.hyperdrive.shared.utils

import org.scalatest.flatspec.AnyFlatSpec

import java.net.{URL, URLClassLoader}
import za.co.absa.hyperdrive.shared.utils.classloaderutils.{TestClassLoaderUtilsNoSingleton, TestClassLoaderUtilsNotInstanceOf, TestClassLoaderUtilsSingletonInstanceOf, TestClassLoaderUtilsTestTrait}

class TestClassLoaderUtils extends AnyFlatSpec {

  behavior of s"${ClassLoaderUtils.getClass.getSimpleName}.loadSingletonClassOfType"

  it should "return the singleton class for a given trait and fully qualified class name" in {
    // given
    val className = TestClassLoaderUtilsSingletonInstanceOf.getClass.getName
    // when
    val singleton = ClassLoaderUtils.loadSingletonClassOfType[TestClassLoaderUtilsTestTrait](className)
    // then
    assert(singleton.isInstanceOf[TestClassLoaderUtilsTestTrait])
    assert(singleton.equals(TestClassLoaderUtilsSingletonInstanceOf))
  }

  // If this test fails, you might have to check TestClassLoaderUtilsOther.jar and createTestClassLoaderUtilsOtherJar.sh
  it should "return the singleton class for a given trait and fully qualified class name from other jar" in {
    // given

    val jarPath: URL = getClass.getClassLoader.getResource("TestClassLoaderUtilsOther.jar")
    val classLoader = ClassLoaderUtils.getClass.getClassLoader.asInstanceOf[URLClassLoader]
    val method = classLoader.getClass.getSuperclass.getDeclaredMethod("addURL", classOf[URL])
    method.setAccessible(true)
    method.invoke(classLoader, jarPath)

    val className = "za.co.absa.hyperdrive.shared.utils.TestClassLoaderUtilsSingletonInstanceOther"

    // when
    val singleton = ClassLoaderUtils.loadSingletonClassOfType[TestClassLoaderUtilsTestTrait](className)

    // then
    assert(singleton.isInstanceOf[TestClassLoaderUtilsTestTrait])
  }

  it should "throw if the given class does not exist" in {
    // given
    val className = "non-existent-class"
    // when
    val throwable = intercept[IllegalArgumentException](ClassLoaderUtils.loadSingletonClassOfType[TestClassLoaderUtilsTestTrait](className))
    // then
    assert(throwable.getMessage.equals("Class 'non-existent-class' could not be found"))
  }

  it should "throw if the given class does not implement the given trait" in {
    // given
    val className = TestClassLoaderUtilsNotInstanceOf.getClass.getName
    // when
    val throwable = intercept[IllegalArgumentException](ClassLoaderUtils.loadSingletonClassOfType[TestClassLoaderUtilsTestTrait](className))
    // then
    assert(throwable.getMessage.matches("Class '.*TestClassLoaderUtilsNotInstanceOf.' is not an instance of '.*TestClassLoaderUtilsTestTrait'"))
  }

  it should "throw if the given class is not a singleton" in {
    // given
    val className = (new TestClassLoaderUtilsNoSingleton).getClass.getName
    // when
    val throwable = intercept[IllegalArgumentException](ClassLoaderUtils.loadSingletonClassOfType[TestClassLoaderUtilsTestTrait](className))
    // then
    assert(throwable.getMessage.contains("TestClassLoaderUtilsNoSingleton' is not a singleton"))
  }

}
