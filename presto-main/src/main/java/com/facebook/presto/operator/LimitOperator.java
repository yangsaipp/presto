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
package com.facebook.presto.operator;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.plan.PlanNodeId;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class LimitOperator
        implements Operator
{
    public static class LimitOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final long limit;
        private final long offset;
        private final boolean partial;
        private boolean closed;
        

        public LimitOperatorFactory(int operatorId, PlanNodeId planNodeId, long limit, long offset, boolean partial)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.limit = limit;
            this.offset = offset;
            this.partial = partial;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LimitOperator.class.getSimpleName());
            // partial 阶段，进行 rewrite
            if(partial) {
            	System.out.println(String.format("==== createOperator...。partial=%b, offset=%d, limit=%d", partial, offset, limit));
            	return new LimitOperator(operatorContext, offset + limit, 0);
            }
            System.out.println(String.format("==== createOperator。partial=%b, offset=%d, limit=%d",partial, offset, limit));
            return new LimitOperator(operatorContext, limit, offset);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new LimitOperatorFactory(operatorId, planNodeId, limit, offset, partial);
        }
    }

    private final OperatorContext operatorContext;
    private Page nextPage;
    private long remainingLimit;
    private long offset;

    public LimitOperator(OperatorContext operatorContext, long limit, long offset)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");

        checkArgument(limit >= 0, "limit must be at least zero");
        this.offset = offset;
        this.remainingLimit = limit;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        remainingLimit = 0;
    }

    @Override
    public boolean isFinished()
    {
        return remainingLimit == 0 && nextPage == null;
    }

    @Override
    public boolean needsInput()
    {
        return remainingLimit > 0 && nextPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(needsInput());
        System.out.println(String.format("==== addInput...。pageCount=%d, offset=%d, limit=%d, this=%s", page.getPositionCount(), offset, remainingLimit, this));
        // 如果当前的偏移量大于当前page的行数，则重置偏移量
        if (offset >= page.getPositionCount()) {
            offset -= page.getPositionCount();
        }else {
        	if (offset == 0 && remainingLimit >= page.getPositionCount()) {
                nextPage = page;
                remainingLimit -= page.getPositionCount();
            } else {
                // 当前page还剩余多少行元素
                long remainingPosition = (int) (page.getPositionCount() - offset);

                // 如果用户需要的limit的数量大于等于当前page剩余的行数，则当前page只能提供remainingPosition行
                // cntInPage为当前page提供的行数, 通常情况下, remainingLimit >= remainingPosition = true
                long cntInPage = remainingLimit >= remainingPosition ? remainingPosition : remainingLimit;

                Block[] blocks = new Block[page.getChannelCount()];
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    Block block = page.getBlock(channel);
                    blocks[channel] = block.getRegion((int) offset, (int) cntInPage);
                }
                nextPage = new Page((int) cntInPage, blocks);
                // 用户还需要多少limit元素，即 offset X limit Y 中的 Y 的个数
                remainingLimit -= cntInPage;
                // 把当前的偏移量置成 0
                offset = 0;
            }
        }
    }

    @Override
    public Page getOutput()
    {
        Page page = nextPage;
        nextPage = null;
        return page;
    }
}
