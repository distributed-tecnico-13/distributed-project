package didatrade.util;

import java.util.ArrayList;

/**
 * Judges replies as they arrive. onNext returns true once the outcome is final, which releases the
 * collector.
 */
public abstract class GenericResponseProcessor<T> {

	abstract boolean onNext (ArrayList<T> all_responses, T last_response);
}
