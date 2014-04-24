package org.elasticsearch;

import org.mozilla.javascript.Context;
import org.mozilla.javascript.ScriptableObject;
import org.testng.annotations.Test;

public class RhinoTest {
	@Test
	public void testScriptEval() {
		final String script = "var num = 5; num += 4; 'SELECT * FORM [' + num + ']'";
		final Context context = Context.enter();
		final ScriptableObject scope = context.initStandardObjects();
		final String result = Context.toString(context.evaluateString(scope, script, "test", 1, null));
		System.out.println(result);
	}
}
