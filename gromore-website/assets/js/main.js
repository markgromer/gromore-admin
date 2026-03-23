// GroMore Website - Main JS

document.addEventListener('DOMContentLoaded', function () {

    // ── Nav scroll effect ──
    var nav = document.getElementById('mainNav');
    if (nav) {
        window.addEventListener('scroll', function () {
            if (window.scrollY > 40) {
                nav.classList.add('scrolled');
            } else {
                nav.classList.remove('scrolled');
            }
        });
    }

    // ── Mobile menu toggle ──
    var toggle = document.getElementById('navToggle');
    var mobileMenu = document.getElementById('navMobile');
    if (toggle && mobileMenu) {
        toggle.addEventListener('click', function () {
            toggle.classList.toggle('active');
            mobileMenu.classList.toggle('open');
            document.body.classList.toggle('menu-open');
        });
        // Close mobile menu when a link is clicked
        var mobileLinks = mobileMenu.querySelectorAll('a');
        mobileLinks.forEach(function (link) {
            link.addEventListener('click', function () {
                toggle.classList.remove('active');
                mobileMenu.classList.remove('open');
                document.body.classList.remove('menu-open');
            });
        });
    }

    // ── FAQ accordion ──
    var faqButtons = document.querySelectorAll('.faq-question');
    faqButtons.forEach(function (btn) {
        btn.addEventListener('click', function () {
            var item = btn.parentElement;
            var isOpen = item.classList.contains('open');

            // Close all FAQ items in same list
            var siblings = item.parentElement.querySelectorAll('.faq-item');
            siblings.forEach(function (sib) {
                sib.classList.remove('open');
            });

            // Open this one if it was closed
            if (!isOpen) {
                item.classList.add('open');
            }
        });
    });

    // ── Smooth scroll for anchor links ──
    document.querySelectorAll('a[href^="#"]').forEach(function (link) {
        link.addEventListener('click', function (e) {
            var href = link.getAttribute('href');
            if (!href || href === '#') return;
            var target;
            try { target = document.querySelector(href); } catch(ex) { return; }
            if (target) {
                e.preventDefault();
                var navHeight = nav ? nav.offsetHeight : 0;
                var top = target.getBoundingClientRect().top + window.pageYOffset - navHeight - 20;
                window.scrollTo({ top: top, behavior: 'smooth' });
            }
        });
    });

    // ── Contact form handling ──
    var contactForm = document.getElementById('contactForm');
    var formSuccess = document.getElementById('formSuccess');
    if (contactForm && formSuccess) {
        contactForm.addEventListener('submit', function (e) {
            e.preventDefault();

            // Basic client-side validation is handled by required attributes
            // In production, replace this with actual form submission (e.g. Formspree, Netlify Forms, or API endpoint)

            // Show success message
            contactForm.style.display = 'none';
            formSuccess.style.display = 'block';

            // Scroll to success message
            formSuccess.scrollIntoView({ behavior: 'smooth', block: 'center' });
        });
    }

    // ── Intersection Observer for fade-in animations ──
    if ('IntersectionObserver' in window) {
        var observer = new IntersectionObserver(function (entries) {
            entries.forEach(function (entry) {
                if (entry.isIntersecting) {
                    entry.target.classList.add('visible');
                    observer.unobserve(entry.target);
                }
            });
        }, { threshold: 0.1, rootMargin: '0px 0px -40px 0px' });

        // Observe feature cards, step cards, split sections, pricing cards, testimonial cards
        var animateElements = document.querySelectorAll(
            '.feature-card, .step-card, .split-section, .pricing-card, .testimonial-card, .value-card, .contact-info-card'
        );
        animateElements.forEach(function (el) {
            el.classList.add('fade-in');
            observer.observe(el);
        });
    }

});
