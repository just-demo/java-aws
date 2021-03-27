package demo.jpa;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.AuditorAware;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import java.util.Optional;

import static java.util.Optional.of;

@Component
public class VisitAuditor implements AuditorAware<String> {
    @Autowired
    private HttpServletRequest request;

    public Optional<String> getCurrentAuditor() {
        return of(request.getRemoteAddr());
    }
}