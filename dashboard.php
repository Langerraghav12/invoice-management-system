<?php
include('header.php');
include('functions.php');
include_once("includes/config.php");

// Fetch Sales Amount
$salesResult = mysqli_query($mysqli, 'SELECT SUM(subtotal) AS value_sum FROM invoices WHERE status = "paid"'); 
$salesRow = mysqli_fetch_assoc($salesResult); 
$salesAmount = $salesRow['value_sum'] ? $salesRow['value_sum'] : 0;

// Fetch Due Amount
$dueResult = mysqli_query($mysqli, 'SELECT SUM(subtotal) AS value_sum FROM invoices WHERE status = "open"'); 
$dueRow = mysqli_fetch_assoc($dueResult); 
$dueAmount = $dueRow['value_sum'] ? $dueRow['value_sum'] : 0;
?>

<section class="content">
    <!-- Dashboard Overview -->
    <div class="row">
        <!-- Sales Amount -->
        <div class="col-lg-3 col-xs-6">
            <div class="small-box shadow" style="background: linear-gradient(135deg, #43a047, #66bb6a); color: #fff;">
                <div class="inner">
                    <h3>
                        <?php 
                        echo $salesAmount ? '$' . number_format($salesAmount, 2) : '$0.00';
                        ?>
                    </h3>
                    <p>Sales Amount</p>
                </div>
                <div class="icon">
                    <i class="ion ion-social-usd" style="color: #fff; opacity: 0.7;"></i>
                </div>
            </div>
        </div>

        <!-- Total Invoices -->
        <div class="col-lg-3 col-xs-6">
            <div class="small-box shadow" style="background: linear-gradient(135deg, #8e24aa, #ba68c8); color: #fff;">
                <div class="inner">
                    <h3>
                        <?php 
                        $sql = "SELECT * FROM invoices";
                        $query = $mysqli->query($sql);
                        echo $query->num_rows;
                        ?>
                    </h3>
                    <p>Total Invoices</p>
                </div>
                <div class="icon">
                    <i class="ion ion-printer" style="color: #fff; opacity: 0.7;"></i>
                </div>
                <a href="invoice-list.php" class="small-box-footer">
                    Manage Invoices <i class="fa fa-arrow-circle-right"></i>
                </a>
            </div>
        </div>

        <!-- Pending Bills -->
        <div class="col-lg-3 col-xs-6">
            <div class="small-box shadow" style="background: linear-gradient(135deg, #f57f17, #ffb300); color: #fff;">
                <div class="inner">
                    <h3>
                        <?php 
                        $sql = "SELECT * FROM invoices WHERE status = 'open'";
                        $query = $mysqli->query($sql);
                        echo $query->num_rows;
                        ?>
                    </h3>
                    <p>Pending Bills</p>
                </div>
                <div class="icon">
                    <i class="ion ion-load-a" style="color: #fff; opacity: 0.7;"></i>
                </div>
                <a href="invoice-list.php" class="small-box-footer">
                    View Pending Bills <i class="fa fa-arrow-circle-right"></i>
                </a>
            </div>
        </div>

        <!-- Due Amount -->
        <div class="col-lg-3 col-xs-6">
            <div class="small-box shadow" style="background: linear-gradient(135deg, #d32f2f, #e57373); color: #fff;">
                <div class="inner">
                    <h3>
                        <?php 
                        echo $dueAmount ? '$' . number_format($dueAmount, 2) : '$0.00';
                        ?>
                    </h3>
                    <p>Due Amount</p>
                </div>
                <div class="icon">
                    <i class="ion ion-alert-circled" style="color: #fff; opacity: 0.7;"></i>
                </div>
                <a href="invoice-list.php" class="small-box-footer">
                    Resolve Dues <i class="fa fa-arrow-circle-right"></i>
                </a>
            </div>
        </div>
    </div>

    <!-- Secondary Stats -->
    <div class="row">
        <!-- Total Products -->
        <div class="col-lg-3 col-xs-6">
            <div class="small-box shadow" style="background: linear-gradient(135deg, #1565c0, #42a5f5); color: #fff;">
                <div class="inner">
                    <h3>
                        <?php 
                        $sql = "SELECT * FROM products";
                        $query = $mysqli->query($sql);
                        echo $query->num_rows;
                        ?>
                    </h3>
                    <p>Total Products</p>
                </div>
                <div class="icon">
                    <i class="ion ion-social-dropbox" style="color: #fff; opacity: 0.7;"></i>
                </div>
                <a href="product-list.php" class="small-box-footer">
                    View Products <i class="fa fa-arrow-circle-right"></i>
                </a>
            </div>
        </div>

        <!-- Total Customers -->
        <div class="col-lg-3 col-xs-6">
            <div class="small-box shadow" style="background: linear-gradient(135deg, #4e342e, #8d6e63); color: #fff;">
                <div class="inner">
                    <h3>
                        <?php 
                        $sql = "SELECT * FROM store_customers";
                        $query = $mysqli->query($sql);
                        echo $query->num_rows;
                        ?>
                    </h3>
                    <p>Total Customers</p>
                </div>
                <div class="icon">
                    <i class="ion ion-ios-people" style="color: #fff; opacity: 0.7;"></i>
                </div>
                <a href="customer-list.php" class="small-box-footer">
                    Manage Customers <i class="fa fa-arrow-circle-right"></i>
                </a>
            </div>
        </div>

        <!-- Paid Bills -->
        <div class="col-lg-3 col-xs-6">
            <div class="small-box shadow" style="background: linear-gradient(135deg, #33691e, #689f38); color: #fff;">
                <div class="inner">
                    <h3>
                        <?php 
                        $sql = "SELECT * FROM invoices WHERE status = 'paid'";
                        $query = $mysqli->query($sql);
                        echo $query->num_rows;
                        ?>
                    </h3>
                    <p>Paid Bills</p>
                </div>
                <div class="icon">
                    <i class="ion ion-ios-paper" style="color: #fff; opacity: 0.7;"></i>
                </div>
                <a href="invoice-list.php" class="small-box-footer">
                    View Paid Bills <i class="fa fa-arrow-circle-right"></i>
                </a>
            </div>
        </div>
    </div>

    <!-- Graph Section -->
    <div class="row">
        <div class="col-lg-12">
            <div class="card">
                <div class="card-header">
                    <h3>Sales vs Due Amount</h3>
                </div>
                <div class="card-body">
                    <canvas id="salesDueChart"></canvas>
                </div>
            </div>
        </div>
    </div>
</section>

<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script>
    // Prepare data for the chart
    const ctx = document.getElementById('salesDueChart').getContext('2d');
    const salesDueChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['Sales Amount', 'Due Amount'],
            datasets: [{
                label: 'Amount in USD',
                data: [<?php echo $salesAmount; ?>, <?php echo $dueAmount; ?>],
                backgroundColor: [
                    'rgba(67, 160, 71, 0.7)', // Green for Sales
                    'rgba(211, 47, 47, 0.7)'  // Red for Due
                ],
                borderColor: [
                    'rgba(67, 160, 71, 1)',
                    'rgba(211, 47, 47, 1)'
                ],
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                },
            },
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        }
    });
</script>

<?php
include('footer.php');
?>

