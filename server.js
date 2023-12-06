const express = require('express');
const mongoose = require('mongoose');
const multer = require('multer');
const csvParser = require('csv-parser');
const cors = require('cors');
const fs = require('fs');

const app = express();
app.use(cors());

// Connect to MongoDB
const conn_string = 'mongodb+srv://username:password@cluster0.u6wy93z.mongodb.net/dbmsproj?retryWrites=true&w=majority'
mongoose.connect(conn_string, { useNewUrlParser: true, useUnifiedTopology: true })
.then(
  console.log('connected to mongo db')
)
.catch((err) => console.log(err));

// Define a schema for the collection where CSV data will be stored
const Schema = mongoose.Schema;
const transaction_schema = new Schema({
   transaction_id: { type: Number, required: true, index: true },
   timestamp: { type: Date, required: true },
   customer_id: { type: Number, required: true},
   product_id: { type: Number, required: true},
   employee_id: { type: Number, required: true },
   payment_id: { type: Number, required: true}, 
   quantity: { type: Number, required: true},
   total_amount: { type: Number, required: true}
},{strict: false});

const customer_schema = new Schema({
  customer_id: { type: Number, required: true, index: true },
  customer_first_name: { type: String, required: true},
  customer_last_name: { type: String, required: true},
  customer_city: { type: String, required: true },
  customer_state: { type: String, required: true}, 
  customer_postal: { type: Number, required: true},
  customer_email: { type: String, required: true},
  customer_phone: { type: String, required: true},
},{strict: false});

const employee_schema = new Schema({
  employee_id: { type: Number, required: true, index: true },
  employee_name: { type: String, required: true},
  employee_ssn: { type: String, required: true},
  employee_phone : { type: String, required: true },
  employee_state: { type: String, required: true}, 
  employee_city: { type: String, required: true}, 
  employee_postal: { type: Number, required: true},
},{strict: false});

const product_schema = new Schema({
  product_id: { type: Number, required: true, index: true },
  product_name: { type: String, required: true},
  product_category: { type: String, required: true},
  unit_price: { type: Number, required: true },
},{strict: false});

const payment_schema = new Schema({
  payment_id: { type: Number, required: true, index: true },
  payment_type: { type: String, required: true},
  
},{strict: false});

const CSV_schema = new Schema({
  customer_id: { type: Number, required: true, index: true },
  customer_first_name: { type: String, required: true},
  customer_last_name: { type: String, required: true},
  customer_city: { type: String, required: true },
  customer_state: { type: String, required: true}, 
  customer_postal: { type: Number, required: true},
  customer_email: { type: String, required: true},
  customer_phone: { type: String, required: true},
},{strict: false});


const transaction = mongoose.model('Transaction', transaction_schema);
const customer = mongoose.model('Customer', customer_schema);
const employee = mongoose.model('Employee', employee_schema);
const product = mongoose.model('Product', product_schema);
const payment = mongoose.model('Payment', payment_schema);
// Multer setup for file uploads
const upload = multer({ dest: 'uploads/' });

// Route for file upload - transactions
app.post('/api/upload/Transaction', upload.single('file'), (req, res) => {
  const results = [];

  fs.createReadStream(req.file.path)
    .pipe(csvParser())
    .on('data', (data) => {
      
          results.push(data);
      
  })
    .on('end', () => {
      // Insert into MongoDB
      transaction.deleteMany({})
      .then(() =>{
            return transaction.insertMany(results);
      })
            .then((results) => { 
                //console.log(results);
                fs.unlinkSync(req.file.path); // Remove the file after saving to DB
                res.status(200).json({ message: 'Data inserted successfully' })})
            .catch((error) => res.status(500).json({ error }));
            
    });
});
// Route for file upload - customer
app.post('/api/upload/Customer', upload.single('file'), (req, res) => {
  const results = [];

  fs.createReadStream(req.file.path)
    .pipe(csvParser())
    .on('data', (data) => {
      
          results.push(data);
      
  })
    .on('end', () => {
      // Insert into MongoDB
      customer.deleteMany({})
      .then(() =>{
            return customer.insertMany(results);
      })
            .then((results) => { 
                //console.log(results);
                fs.unlinkSync(req.file.path); // Remove the file after saving to DB
                res.status(200).json({ message: 'Data inserted successfully' })})
            .catch((error) => res.status(500).json({ error }));
            
    });
});
// Route for file upload - employee
app.post('/api/upload/Employee', upload.single('file'), (req, res) => {
  const results = [];

  fs.createReadStream(req.file.path)
    .pipe(csvParser())
    .on('data', (data) => {
      
          results.push(data);
      
  })
    .on('end', () => {
      // Insert into MongoDB
      employee.deleteMany({})
      .then(() =>{
            return employee.insertMany(results);
      })
            .then((results) => { 
                //console.log(results);
                fs.unlinkSync(req.file.path); // Remove the file after saving to DB
                res.status(200).json({ message: 'Data inserted successfully' })})
            .catch((error) => res.status(500).json({ error }));
            
    });
});
// Route for file upload - product
app.post('/api/upload/Product', upload.single('file'), (req, res) => {
  const results = [];

  fs.createReadStream(req.file.path)
    .pipe(csvParser())
    .on('data', (data) => {
      
          results.push(data);
      
  })
    .on('end', () => {
      // Insert into MongoDB
      product.deleteMany({})
      .then(() =>{
            return product.insertMany(results);
      })
            .then((results) => { 
                //console.log(results);
                fs.unlinkSync(req.file.path); // Remove the file after saving to DB
                res.status(200).json({ message: 'Data inserted successfully' })})
            .catch((error) => res.status(500).json({ error }));
            
    });
});
// Route for file upload - payment
app.post('/api/upload/Payment', upload.single('file'), (req, res) => {
  const results = [];

  fs.createReadStream(req.file.path)
    .pipe(csvParser())
    .on('data', (data) => {
      
          results.push(data);
      
  })
    .on('end', () => {
      // Insert into MongoDB
      payment.deleteMany({})
      .then(() =>{
            return payment.insertMany(results);
      })
            .then((results) => { 
                //console.log(results);
                fs.unlinkSync(req.file.path); // Remove the file after saving to DB
                res.status(200).json({ message: 'Data inserted successfully' })})
            .catch((error) => res.status(500).json({ error }));
            
    });
});
// Start the server
const PORT = 4000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
