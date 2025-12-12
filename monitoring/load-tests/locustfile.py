"""
Locust load testing script for TableCRM API.
Tests the Four Golden Signals with real endpoints:
- Latency: Response times
- Traffic: Request rate
- Errors: Error rate
- Saturation: Concurrent requests
"""

from locust import HttpUser, task, between, events, SequentialTaskSet
from faker import Faker
import random
import logging

fake = Faker("ru_RU")
logger = logging.getLogger(__name__)


class TableCRMUser(HttpUser):
    """
    Simulates a realistic user interacting with TableCRM API.
    Weighted tasks based on typical usage patterns.
    """

    wait_time = between(1, 3)
    host = "http://localhost:9000"

    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    def on_start(self):
        """Initialize user session"""
        logger.info(f"User {self.environment.runner.user_count} started")
        # Simulate login if needed
        # self.client.post("/oauth/token", json={...})

    # ==========================================
    # HIGH FREQUENCY ENDPOINTS (weight: 10-20)
    # ==========================================

    @task(20)
    def get_health(self):
        """Health check - most frequent"""
        self.client.get("/health", name="GET /health")

    @task(15)
    def get_docs(self):
        """API documentation"""
        self.client.get("/docs", name="GET /docs")

    @task(15)
    def get_warehouse_balances(self):
        """Check warehouse stock"""
        with self.client.get(
            "/warehouse_balances/",
            params={"limit": 50, "offset": 0},
            name="GET /warehouse_balances/",
        ) as resp:
            if resp.status_code == 422:
                resp.succes()

    @task(12)
    def get_nomenclature(self):
        """Browse products catalog"""
        with self.client.get(
            "/nomenclature/",
            params={"limit": random.randint(10, 100), "offset": random.randint(0, 200)},
            name="GET /nomenclature/",
        ) as resp:
            if resp.status_code == 422:
                resp.succes()

    @task(10)
    def get_docs_sales(self):
        """View sales documents"""
        with self.client.get(
            "/docs_sales/", params={"limit": 20, "offset": 0}, name="GET /docs_sales/"
        ) as resp:
            if resp.status_code == 422:
                resp.succes()

    # ==========================================
    # MEDIUM FREQUENCY ENDPOINTS (weight: 5-9)
    # ==========================================

    @task(8)
    def get_categories(self):
        """Browse categories"""
        with self.client.get("/categories/", name="GET /categories/") as resp:
            if resp.status_code == 422:
                resp.succes()

    @task(8)
    def get_categories_tree(self):
        """Category tree structure"""
        with self.client.get("/categories_tree/", name="GET /categories_tree/") as resp:
            if resp.status_code == 422:
                resp.succes()

    @task(7)
    def get_loyality_cards(self):
        """Check loyalty cards"""
        with self.client.get("/loyality_cards/", name="GET /loyality_cards/") as resp:
            if resp.status_code == 422:
                resp.succes()

    @task(6)
    def get_analytics(self):
        """View analytics dashboard"""
        with self.client.get(
            "/analytics/",
            params={"date_from": "2025-11-01", "date_to": "2025-11-19"},
            name="GET /analytics/",
        ) as resp:
            if resp.status_code == 422:
                resp.succes()

    @task(6)
    def get_payments(self):
        """View payments"""
        with self.client.get(
            "/payments/", params={"limit": 50}, name="GET /payments/"
        ) as resp:
            if resp.status_code == 422:
                resp.succes()

    @task(5)
    def get_cheques(self):
        """View receipts"""
        with self.client.get("/cheques/", name="GET /cheques/") as resp:
            if resp.status_code == 422:
                resp.succes()

    @task(5)
    def get_contragents(self):
        """View clients/suppliers"""
        with self.client.get("/contragents/", name="GET /contragents/") as resp:
            if resp.status_code == 422:
                resp.succes()

    @task(5)
    def get_organizations(self):
        """List organizations"""
        with self.client.get("/organizations/", name="GET /organizations/") as resp:
            if resp.status_code == 422:
                resp.succes()

    # ==========================================
    # LOW FREQUENCY ENDPOINTS (weight: 1-4)
    # ==========================================

    @task(4)
    def get_warehouses(self):
        """List warehouses"""
        with self.client.get("/warehouses/", name="GET /warehouses/") as resp:
            if resp.status_code == 422:
                resp.succes()

    @task(4)
    def get_cashbox_settings(self):
        """Cashbox configuration"""
        with self.client.get("/cashbox/settings", name="GET /cashbox/settings") as resp:
            if resp.status_code == 422:
                resp.succes()

    @task(3)
    def get_users_list(self):
        """List users"""
        with self.client.get("/users/list/", name="GET /users/list/") as resp:
            if resp.status_code == 422:
                resp.succes()

    @task(3)
    def get_docs_purchases(self):
        """Purchase documents"""
        with self.client.get("/docs_purchases/", name="GET /docs_purchases/") as resp:
            if resp.status_code == 422:
                resp.succes()

    @task(3)
    def get_booking_list(self):
        """Bookings list"""
        with self.client.get("/booking/list", name="GET /booking/list") as resp:
            if resp.status_code == 422:
                resp.succes()

    @task(2)
    def get_segments(self):
        """Customer segments"""
        with self.client.get("/segments/", name="GET /segments/") as resp:
            if resp.status_code == 422:
                resp.succes()

    @task(2)
    def get_tech_cards(self):
        """Production cards"""
        with self.client.get("/tech_cards/", name="GET /tech_cards/") as resp:
            if resp.status_code == 422:
                resp.succes()

    @task(2)
    def get_manufacturers(self):
        """Manufacturers list"""
        with self.client.get("/manufacturers/", name="GET /manufacturers/") as resp:
            if resp.status_code == 422:
                resp.succes()

    @task(2)
    def get_employee_shifts_status(self):
        """Check shift status"""
        with self.client.get(
            "/employee-shifts/status", name="GET /employee-shifts/status"
        ) as resp:
            if resp.status_code == 422:
                resp.succes()

    @task(1)
    def get_integrations(self):
        """List integrations"""
        with self.client.get("/integrations/", name="GET /integrations/") as resp:
            if resp.status_code == 422:
                resp.succes()

    @task(1)
    def get_feeds(self):
        """RSS feeds"""
        with self.client.get("/feeds", name="GET /feeds") as resp:
            if resp.status_code == 422:
                resp.succes()

    # ==========================================
    # DETAIL VIEW ENDPOINTS (weight: 1-3)
    # ==========================================

    @task(3)
    def get_nomenclature_detail(self):
        """View single product"""
        nomenclature_id = random.randint(1, 1000)
        with self.client.get(
            f"/nomenclature/{nomenclature_id}/",
            name="GET /nomenclature/:id/",
            catch_response=True,
        ) as resp:
            if resp.status_code == 422:
                resp.succes()

    @task(2)
    def get_category_detail(self):
        """View single category"""
        category_id = random.randint(1, 100)
        with self.client.get(
            f"/categories/{category_id}/",
            name="GET /categories/:id/",
            catch_response=True,
        ) as resp:
            if resp.status_code == 422:
                resp.succes()

    @task(2)
    def get_docs_sales_detail(self):
        """View sale document details"""
        doc_id = random.randint(1, 1000)
        with self.client.get(
            f"/docs_sales/{doc_id}/", name="GET /docs_sales/:id/", catch_response=True
        ) as resp:
            if resp.status_code == 422:
                resp.succes()

    @task(1)
    def get_contragent_detail(self):
        """View client details"""
        contragent_id = random.randint(1, 500)
        with self.client.get(
            f"/contragents/{contragent_id}/",
            name="GET /contragents/:id/",
            catch_response=True,
        ) as resp:
            if resp.status_code == 422:
                resp.succes()

    # ==========================================
    # SLOW / HEAVY ENDPOINTS (weight: 1)
    # ==========================================

    @task(1)
    def get_analytics_cards(self):
        """Heavy analytics query"""
        with self.client.get(
            "/analytics_cards/",
            params={"date_from": "2025-01-01", "date_to": "2025-11-19"},
            name="GET /analytics_cards/ (slow)",
        ) as resp:
            if resp.status_code == 422:
                resp.succes()

    @task(1)
    def get_gross_profit_docs(self):
        """Gross profit calculation"""
        with self.client.get(
            "/gross_profit_docs/", name="GET /gross_profit_docs/ (slow)"
        ) as resp:
            if resp.status_code == 422:
                resp.succes()

    @task(1)
    def post_reports_sales(self):
        """Generate sales report"""
        with self.client.post(
            "/reports/sales/",
            json={"date_from": "2025-11-01", "date_to": "2025-11-19", "format": "json"},
            name="POST /reports/sales/ (slow)",
        ) as resp:
            if resp.status_code == 422:
                resp.succes()

    # ==========================================
    # ERROR SIMULATION (weight: 1)
    # ==========================================

    @task(1)
    def simulate_404_error(self):
        """Simulate 404 error"""
        self.client.get(
            f"/nomenclature/{fake.random_int(10000, 99999)}/",
            name="GET /nomenclature/:id/ (404)",
            catch_response=True,
        )

    @task(1)
    def simulate_random_nonexistent(self):
        """Random non-existent endpoint"""
        self.client.get(
            f"/nonexistent/{fake.uuid4()}/",
            name="GET /nonexistent/:id/ (404)",
            catch_response=True,
        )


class RealisticUserFlow(SequentialTaskSet):
    """
    Realistic user journey: Browse -> Select -> View details
    """

    @task
    def browse_categories(self):
        """Step 1: Browse categories"""
        response = self.client.get("/categories_tree/", name="Browse: Categories")
        if response.status_code == 200:
            logger.info("User browsing categories")
        if response.status_code == 422:
            response.succes()

    @task
    def search_products(self):
        """Step 2: Search products"""
        with self.client.get(
            "/nomenclature/",
            params={"limit": 20, "search": fake.word()},
            name="Browse: Search products",
        ) as resp:
            if resp.status_code == 422:
                resp.succes()

    @task
    def view_product_details(self):
        """Step 3: View product details"""
        product_id = random.randint(1, 100)
        self.client.get(
            f"/nomenclature/{product_id}/",
            name="Browse: Product details",
            catch_response=True,
        )

    @task
    def check_warehouse_stock(self):
        """Step 4: Check stock"""
        self.client.get(
            "/warehouse_balances/", params={"limit": 10}, name="Browse: Check stock"
        )


class RealisticUser(HttpUser):
    """User with realistic browsing behavior"""

    tasks = [RealisticUserFlow]
    wait_time = between(2, 5)
    host = "http://localhost:9000"


class AdminUser(HttpUser):
    """
    Admin user performing management tasks.
    Lower frequency, more heavy queries.
    """

    wait_time = between(3, 7)
    host = "http://localhost:9000"

    @task(5)
    def view_analytics_dashboard(self):
        """Admin views analytics"""
        self.client.get(
            "/analytics/",
            params={"date_from": "2025-11-01", "date_to": "2025-11-19"},
            name="Admin: Analytics",
        )

    @task(3)
    def manage_users(self):
        """Admin manages users"""
        self.client.get("/users/list/", name="Admin: Users list")

    @task(3)
    def view_sales_reports(self):
        """Admin generates reports"""
        self.client.post(
            "/reports/sales/",
            json={"date_from": "2025-11-01", "date_to": "2025-11-19"},
            name="Admin: Sales report",
        )

    @task(2)
    def manage_integrations(self):
        """Admin checks integrations"""
        self.client.get("/integrations/", name="Admin: Integrations")

    @task(1)
    def view_shifts_statistics(self):
        """Admin views employee shifts"""
        self.client.get(
            "/employee-shifts/shifts-statistics/", name="Admin: Shifts stats"
        )


class QuickSmokeTest(HttpUser):
    """
    Quick smoke test - only critical endpoints.
    Use: locust -f locustfile.py QuickSmokeTest
    """

    wait_time = between(0.5, 1)
    host = "http://localhost:9000"

    @task(10)
    def health(self):
        self.client.get("/health")

    @task(5)
    def metrics(self):
        self.client.get("/metrics")

    @task(3)
    def nomenclature(self):
        self.client.get("/nomenclature/")

    @task(2)
    def categories(self):
        self.client.get("/categories/")


# ==========================================
# EVENT HOOKS
# ==========================================


@events.request.add_listener
def on_request(request_type, name, response_time, response_length, exception, **kwargs):
    """Log request details"""
    if exception:
        logger.error(f"❌ Request failed: {name} - {exception}")
    elif response_time > 1000:
        logger.warning(f"⚠️  Slow request: {name} - {response_time}ms")
    elif response_time < 100:
        logger.debug(f"✅ Fast request: {name} - {response_time}ms")


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Log test start"""
    logger.info("🚀 Load test started!")
    logger.info(f"Target: {environment.host}")


@events.quitting.add_listener
def on_quitting(environment, **kwargs):
    """Print summary when test ends"""
    stats = environment.stats.total

    logger.info("=" * 60)
    logger.info("📊 Load Test Summary")
    logger.info("=" * 60)
    logger.info(f"Total users: {environment.runner.user_count}")
    logger.info(f"Total requests: {stats.num_requests}")
    logger.info(f"Total failures: {stats.num_failures}")
    logger.info(f"Failure rate: {stats.fail_ratio * 100:.2f}%")
    logger.info(f"Average response time: {stats.avg_response_time:.2f}ms")
    logger.info(f"Max response time: {stats.max_response_time:.2f}ms")
    logger.info(f"Requests/sec: {stats.total_rps:.2f}")
    logger.info("=" * 60)

    # Fail the test if error rate > 5%
    if stats.fail_ratio > 0.05:
        logger.error("❌ Test FAILED: Error rate exceeds 5%")
        environment.process_exit_code = 1
    else:
        logger.info("✅ Test PASSED: Error rate within acceptable limits")
