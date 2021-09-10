package io.managed.services.test.quickstarts;

import io.cucumber.testng.AbstractTestNGCucumberTests;
import io.cucumber.testng.CucumberOptions;
import org.testng.annotations.DataProvider;

@CucumberOptions()
public class CucumberQuickstartsTest extends AbstractTestNGCucumberTests {

    @SuppressWarnings("EmptyMethod")
    @DataProvider(parallel = true)
    @Override
    public Object[][] scenarios() {
        return super.scenarios();
    }
}
