/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.singleoperator;

import com.facebook.presto.operator.RowComparator;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.planner.CompilerConfig;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.bytecode.instruction.LabelNode;

import javax.inject.Inject;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;
import static com.facebook.presto.util.CompilerUtils.defineClass;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.and;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;

public class TopNCompiler
{
    private final int cacheSize;
    private final LoadingCache<CacheKey, Comparator<Block[]>> comparatorLoadingCache;

    @Inject
    public TopNCompiler(CompilerConfig config)
    {
        this.cacheSize = config.getExpressionCacheSize() / 100;
        this.comparatorLoadingCache = CacheBuilder.newBuilder()
                .recordStats()
                .maximumSize(cacheSize)
                .build(CacheLoader.from(this::compileComparatorInternal));
    }

    public Comparator<Block[]> getComparatorForTopNOperator(List<Type> sortTypes, List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        CacheKey key = new CacheKey(sortOrders, sortTypes, sortChannels);
        //Comparator<Block[]> baseComparator = comparatorLoadingCache.getUnchecked(key);
        Comparator<Block[]> baseComparator = new RowComparator(sortTypes, sortChannels, sortOrders);
        return Ordering.from(baseComparator).reverse();
    }

    private Comparator<Block[]> compileComparatorInternal(CacheKey cacheKey)
    {
        return generateComparator(cacheKey.getSortTypes(), cacheKey.getSortChannels(), cacheKey.getSortOrders());
    }

    private Comparator<Block[]> generateComparator(List<Type> sortTypes, List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        CallSiteBinder callSiteBinder = new CallSiteBinder();
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                "RowComparator_1",
                type(Object.class),
                type(Comparator.class, Block[].class));
        //Method definition
        MethodDefinition constructor = classDefinition.declareConstructor(a(PUBLIC));
        constructor.getBody().comment("super();")
                .append(constructor.getThis())
                .invokeConstructor(Object.class).ret();
        Parameter left = arg("left", Object.class);
        Parameter right = arg("right", Object.class);

        MethodDefinition compare = classDefinition.declareMethod(a(PUBLIC), "compare", type(int.class), ImmutableList.of(left, right));
        BytecodeExpression leftRow = left.cast(Block[].class);
        BytecodeExpression rightRow = right.cast(Block[].class);
        BytecodeBlock body = compare.getBody();
        Scope scope = compare.getScope();
        Variable leftIsNull = scope.declareVariable(boolean.class, "leftIsNull");
        Variable rightIsNull = scope.declareVariable(boolean.class, "rightIsNull");
        Variable returnVariable = scope.createTempVariable(int.class);
        for (int i = 0; i < sortChannels.size(); i++) {
            Type type = sortTypes.get(i);
            int channel = sortChannels.get(i);
            SortOrder sortOrder = sortOrders.get(i);

            BytecodeExpression leftBlock = leftRow.getElement(channel);
            BytecodeExpression rightBlock = rightRow.getElement(channel);

            body.append(generateExpressionForSortOrder(sortOrder, constantType(callSiteBinder, type), leftIsNull, rightIsNull,
                    leftBlock, constantInt(0), rightBlock, constantInt(0)));

            LabelNode equal = new LabelNode("equal");
            body.comment("if (compare != 0) return compare")
                    .dup()
                    .ifZeroGoto(equal)
                    .retInt()
                    .visitLabel(equal)
                    .pop(int.class);
        }
        body.push(0).retInt();

        Class<? extends Comparator> comparator =
                defineClass(classDefinition, Comparator.class, callSiteBinder.getBindings(), getClass().getClassLoader());

        try {
            return (Comparator<Block[]>) comparator.newInstance();
        }
        catch (Exception e) {
            return new RowComparator(sortTypes, sortChannels, sortOrders);
        }
    }

    private BytecodeBlock generateExpressionForSortOrder(SortOrder sortOrder, BytecodeExpression type, Variable leftIsNull, Variable rightIsNull, BytecodeExpression leftBlock, BytecodeExpression leftPosition,
            BytecodeExpression rightBlock, BytecodeExpression rightPosition)
    {
        BytecodeBlock block = new BytecodeBlock();
        block.append(leftIsNull.set(leftBlock.invoke("isNull", boolean.class, leftPosition)));
        block.append(rightIsNull.set(rightBlock.invoke("isNull", boolean.class, rightPosition)));
        LabelNode finalLabel = new LabelNode("final");
        block.append(new IfStatement().condition(and(leftIsNull, rightIsNull)).ifTrue(new BytecodeBlock().push(0).gotoLabel(finalLabel)));
        BytecodeBlock leftIsNullBlock = new BytecodeBlock();
        BytecodeBlock rightIsNullBlock = new BytecodeBlock();
        if (sortOrder.isNullsFirst()) {
            leftIsNullBlock.push(-1).gotoLabel(finalLabel);
            rightIsNullBlock.push(1).gotoLabel(finalLabel);
        }
        else {
            leftIsNullBlock.push(1).gotoLabel(finalLabel);
            rightIsNullBlock.push(-1).gotoLabel(finalLabel);
        }
        block.append(new IfStatement().condition(leftIsNull).ifTrue(leftIsNullBlock));
        block.append(new IfStatement().condition(rightIsNull).ifTrue(rightIsNullBlock));
        block.append(type.invoke("compareTo", int.class, leftBlock, leftPosition, rightBlock, rightPosition));
        if (!sortOrder.isAscending()) {
            block.intNegate();
        }
        block.visitLabel(finalLabel);
        return block;
    }

    public MethodDefinition generateCompareCode(List<Type> sortTypes, List<Integer> sortChannels, List<Integer> actualSortPosition, List<SortOrder> sortOrders,
            ClassDefinition classDefinition, CallSiteBinder callSiteBinder, int threshold)
    {
        Parameter position = arg("position", int.class);
        Parameter leftRow = arg("leftRow", Block[].class);
        Parameter rightRow = arg("rightRow", Block[].class);
        Parameter variableBlockBuilder = arg("variableBlockBuilder", Block.class);
        MethodDefinition compare = classDefinition.declareMethod(a(PUBLIC), "compare", type(int.class), ImmutableList.of(position, leftRow, rightRow, variableBlockBuilder));
        BytecodeBlock body = compare.getBody();
        Scope scope = compare.getScope();
        Variable leftIsNull = scope.declareVariable(boolean.class, "leftIsNull");
        Variable rightIsNull = scope.declareVariable(boolean.class, "rightIsNull");
        for (int i = 0; i < sortChannels.size(); i++) {
            Type type = sortTypes.get(i);
            int channel = sortChannels.get(i);
            SortOrder sortOrder = sortOrders.get(i);

            BytecodeExpression rightBlock = rightRow.getElement(actualSortPosition.get(i));

            if (channel < threshold) {
                BytecodeExpression leftBlock = leftRow.getElement(channel);
                //Code generated for Sort Order
                body.append(generateExpressionForSortOrder(sortOrder, constantType(callSiteBinder, type), leftIsNull, rightIsNull, rightBlock, constantInt(0), leftBlock, position));
            }
            else {
                body.append(generateExpressionForSortOrder(sortOrder, constantType(callSiteBinder, type), leftIsNull, rightIsNull, rightBlock, constantInt(0), variableBlockBuilder, constantInt(channel - threshold)));
            }

            LabelNode equal = new LabelNode("equal");
            body.comment("if (compare != 0) return compare")
                    .intNegate()
                    .dup()
                    .ifZeroGoto(equal)
                    .retInt()
                    .visitLabel(equal)
                    .pop(int.class);
        }
        body.push(0).retInt();
        return compare;
    }

    public static class CacheKey
    {
        private final List<SortOrder> sortOrders;
        private final List<Type> sortTypes;
        private final List<Integer> sortChannels;

        public CacheKey(List<SortOrder> sortOrders, List<Type> sortTypes, List<Integer> sortChannels)
        {
            this.sortOrders = ImmutableList.copyOf(sortOrders);
            this.sortTypes = ImmutableList.copyOf(sortTypes);
            this.sortChannels = ImmutableList.copyOf(sortChannels);
        }

        public List<SortOrder> getSortOrders()
        {
            return sortOrders;
        }

        public List<Type> getSortTypes()
        {
            return sortTypes;
        }

        public List<Integer> getSortChannels()
        {
            return sortChannels;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sortOrders, sortChannels, sortTypes);
        }

        @Override
        public boolean equals(Object object)
        {
            if (this == object) {
                return true;
            }
            if (object == null || object.getClass() != this.getClass()) {
                return false;
            }
            CacheKey key = (CacheKey) object;
            return Objects.equals(key.sortChannels, this.sortChannels) &&
                    Objects.equals(key.sortOrders, this.sortOrders) &&
                    Objects.equals(key.sortTypes, this.sortTypes);
        }
    }
}
