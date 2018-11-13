/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.contrib.function;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;

public class CreditCardFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CreditCardFunctions.class);

  private CreditCardFunctions() {
  }

  @FunctionTemplate(name = "is_valid_credit_card", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class IsValidCreditCardFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder rawCardNumber;

    @Output
    BitHolder out;

    @Inject
    DrillBuf buffer;

    public void setup() {
    }

    /**
     * The eval() function is where you actually perform the calculation.
     * The eval() function does not return anything.  Instead the return value is returned in the output holder
     * that was declared above.
     * All strings that you get from Drill must be captured using the method demonstrated below
     */
    public void eval() {
      String creditCardNumber = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(rawCardNumber.start, rawCardNumber.end, rawCardNumber.buffer);

      org.apache.commons.validator.routines.CreditCardValidator ccv = new org.apache.commons.validator.routines.CreditCardValidator();

      if (ccv.isValid(creditCardNumber)) {
        out.value = 1;
      } else {
        out.value = 0;
      }
    }
  }
}