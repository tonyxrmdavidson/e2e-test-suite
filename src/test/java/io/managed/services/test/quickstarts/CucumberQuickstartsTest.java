package io.managed.services.test.quickstarts;

import io.cucumber.testng.AbstractTestNGCucumberTests;
import io.cucumber.testng.CucumberOptions;
import org.testng.annotations.DataProvider;

/**
 * <p>
 * <b>Requires:</b>
 * <ul>
 *     <li> PRIMARY_USERNAME
 *     <li> PRIMARY_PASSWORD
 * </ul>
 */
@CucumberOptions()
public class CucumberQuickstartsTest extends AbstractTestNGCucumberTests {

    @SuppressWarnings("EmptyMethod")
    @DataProvider
    @Override
    public Object[][] scenarios() {
        return super.scenarios();
    }
}
