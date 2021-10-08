package INFO7255.assignment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.filter.ShallowEtagHeaderFilter;

import javax.servlet.Filter;

@SpringBootApplication
public class AssignmentApplication {

	//can auto create etag for GET
	@Bean
	public Filter shallowEtagFilter(){
		return new ShallowEtagHeaderFilter();
	}

	public static void main(String[] args) {
		SpringApplication.run(AssignmentApplication.class, args);
	}

}
