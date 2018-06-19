package etl.server.exception.dq;

public class NoSuchRuleException extends Exception {

    public NoSuchRuleException(long jobExecutionId,String rule) {
        super(String.format("Rule can not be found with jobExeuciton id =%s and rule=%s", jobExecutionId, rule));
    }


}
